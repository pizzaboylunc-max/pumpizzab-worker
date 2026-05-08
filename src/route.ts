import "dotenv/config"
import WebSocket from "ws"
import { PrismaClient } from "@prisma/client"

const prisma = new PrismaClient()

const RPC_WS =
  process.env.TERRA_RPC_WS ||
  "wss://terra-classic-rpc.publicnode.com:443/websocket"

const LCD =
  process.env.TERRA_LCD ||
  "https://terra-classic-lcd.publicnode.com"

const PUMPIZZAB_FACTORY =
  "terra1es2n5zp2d4aahfnkrdtq5a7ydjdd0l6ndvpjh66dpqwxtupj0t8qvaxtrw"

const seen = new Set<string>()

function getAttr(event: any, key: string) {
  return event?.attributes?.find((a: any) => a.key === key)?.value
}

function safeNumber(value: any) {
  const n = Number(value)
  return Number.isFinite(n) ? n : 0
}

async function getTx(txHash: string) {
  const res = await fetch(`${LCD}/cosmos/tx/v1beta1/txs/${txHash}`)

  if (!res.ok) {
    throw new Error(`LCD fetch failed ${res.status}: ${txHash}`)
  }

  return res.json()
}

async function isKnownPumpToken(input: {
  token?: string
  contract?: string
  pair?: string
}) {
  const found = await prisma.pumpToken.findFirst({
    where: {
      OR: [
        input.token ? { token: input.token } : undefined,
        input.contract ? { curveContract: input.contract } : undefined,
        input.contract ? { launchContract: input.contract } : undefined,
        input.pair ? { terraportPair: input.pair } : undefined,
      ].filter(Boolean) as any,
    },
  })

  return found
}

async function registerPumpToken(input: {
  token: string
  symbol?: string
  name?: string
  launchContract?: string
  curveContract?: string
  terraportPair?: string
  graduated?: boolean
}) {
  return prisma.pumpToken.upsert({
    where: {
      token: input.token,
    },
    update: {
      symbol: input.symbol,
      name: input.name,
      launchContract: input.launchContract,
      curveContract: input.curveContract,
      terraportPair: input.terraportPair,
      graduated: input.graduated ?? undefined,
    },
    create: {
      token: input.token,
      symbol: input.symbol,
      name: input.name,
      launchContract: input.launchContract,
      curveContract: input.curveContract,
      terraportPair: input.terraportPair,
      graduated: input.graduated ?? false,
    },
  })
}

function extractEvents(tx: any) {
  const responseEvents = tx?.tx_response?.events || []

  const logEvents = (tx?.tx_response?.logs || []).flatMap(
    (log: any) => log.events || []
  )

  return [...responseEvents, ...logEvents]
}

async function detectLaunch(events: any[]) {
  const text = JSON.stringify(events).toLowerCase()

  if (!text.includes(PUMPIZZAB_FACTORY)) {
    return
  }

  const wasm = events.filter((e: any) => e.type === "wasm")

  const tokenEvent = wasm.find((e: any) => {
    return (
      getAttr(e, "token") ||
      getAttr(e, "token_address") ||
      getAttr(e, "token_addr")
    )
  })

  const token =
    getAttr(tokenEvent, "token") ||
    getAttr(tokenEvent, "token_address") ||
    getAttr(tokenEvent, "token_addr")

  if (!token) return

  const curveContract =
    getAttr(tokenEvent, "curve") ||
    getAttr(tokenEvent, "curve_contract") ||
    getAttr(tokenEvent, "pair") ||
    getAttr(tokenEvent, "pair_address") ||
    getAttr(tokenEvent, "_contract_address")

  await registerPumpToken({
    token,
    symbol:
      getAttr(tokenEvent, "symbol") ||
      getAttr(tokenEvent, "token_symbol") ||
      "PUMP",
    name:
      getAttr(tokenEvent, "name") ||
      getAttr(tokenEvent, "token_name"),
    launchContract: PUMPIZZAB_FACTORY,
    curveContract,
    graduated: false,
  })

  console.log("[token-registered]", token)
}

async function detectGraduation(events: any[]) {
  const wasm = events.filter((e: any) => e.type === "wasm")

  const graduationEvent = wasm.find((e: any) => {
    const text = JSON.stringify(e).toLowerCase()

    return (
      text.includes("graduate") ||
      text.includes("graduated") ||
      text.includes("terraport")
    )
  })

  if (!graduationEvent) return

  const token =
    getAttr(graduationEvent, "token") ||
    getAttr(graduationEvent, "token_address") ||
    getAttr(graduationEvent, "asset") ||
    getAttr(graduationEvent, "asset_addr")

  if (!token) return

  const terraportPair =
    getAttr(graduationEvent, "terraport_pair") ||
    getAttr(graduationEvent, "pair") ||
    getAttr(graduationEvent, "pair_address") ||
    getAttr(graduationEvent, "_contract_address")

  await registerPumpToken({
    token,
    terraportPair,
    graduated: true,
  })

  console.log("[graduated]", token, terraportPair)
}

async function parseCurveTrade(tx: any, events: any[]) {
  const txHash = tx?.tx_response?.txhash
  const wasm = events.filter((e: any) => e.type === "wasm")

  const trade = wasm.find((e: any) => {
    const action = getAttr(e, "action")

    return (
      (action === "buy" || action === "sell") &&
      getAttr(e, "amount_in") &&
      getAttr(e, "amount_out") &&
      getAttr(e, "reserve_after") &&
      getAttr(e, "tokens_sold_after")
    )
  })

  if (!trade) return null

  const side = getAttr(trade, "action")
  const pairAddress = getAttr(trade, "_contract_address")

  const transfer = wasm.find((e: any) => {
    return (
      getAttr(e, "action") === "transfer" &&
      getAttr(e, "from") === pairAddress &&
      !!getAttr(e, "to")
    )
  })

  const token = getAttr(transfer, "_contract_address")

  if (!token) return null

  const amountInRaw = safeNumber(getAttr(trade, "amount_in"))
  const amountOutRaw = safeNumber(getAttr(trade, "amount_out"))

  const amountBase =
    side === "buy"
      ? amountInRaw / 1_000_000
      : amountOutRaw / 1_000_000

  const amountToken =
    side === "buy"
      ? amountOutRaw / 1_000_000
      : amountInRaw / 1_000_000

  const known =
    await isKnownPumpToken({
      token,
      contract: pairAddress,
    })

  if (!known) {
    await registerPumpToken({
      token,
      curveContract: pairAddress,
      symbol: "PUMP",
      graduated: false,
    })
  }

  return {
    txHash,
    token,
    symbol: known?.symbol || "PUMP",
    trader:
      getAttr(trade, "buyer") ||
      getAttr(trade, "seller") ||
      "unknown",
    side,
    amountToken,
    amountBase,
    amountUsd: 0,
    priceUsd: 0,
    priceBase: amountToken > 0 ? amountBase / amountToken : 0,
    pairAddress,
  }
}

async function parseTerraportTrade(tx: any, events: any[]) {
  const txHash = tx?.tx_response?.txhash
  const wasm = events.filter((e: any) => e.type === "wasm")

  const swap = wasm.find((e: any) => {
    const action = getAttr(e, "action")
    return action === "swap"
  })

  if (!swap) return null

  const pairAddress = getAttr(swap, "_contract_address")

  const knownPair = await isKnownPumpToken({
    pair: pairAddress,
  })

  const transfers = wasm.filter((e: any) => {
    return getAttr(e, "action") === "transfer"
  })

  const tokenTransfer = transfers.find((e: any) => {
    return getAttr(e, "_contract_address")?.startsWith("terra")
  })

  const token = getAttr(tokenTransfer, "_contract_address")

  const knownToken = await isKnownPumpToken({
    token,
  })

  const known = knownPair || knownToken

  if (!known) return null

  const amountToken =
    safeNumber(getAttr(tokenTransfer, "amount")) / 1_000_000

  return {
    txHash,
    token: known.token,
    symbol: known.symbol || "PUMP",
    trader:
      getAttr(swap, "sender") ||
      getAttr(tokenTransfer, "to") ||
      "unknown",
    side: "buy",
    amountToken,
    amountBase: 0,
    amountUsd: 0,
    priceUsd: 0,
    priceBase: 0,
    pairAddress,
  }
}

async function saveSwap(swap: any) {
  if (!swap?.txHash) return
  if (!swap?.token) return
  if (!swap?.pairAddress) return

  await prisma.swapEvent.upsert({
    where: {
      txHash: swap.txHash,
    },
    update: {},
    create: {
      txHash: swap.txHash,
      token: swap.token,
      symbol: swap.symbol || "PUMP",
      trader: swap.trader || "unknown",
      side: swap.side || "buy",
      amountToken: swap.amountToken || 0,
      amountBase: swap.amountBase || 0,
      amountUsd: swap.amountUsd || 0,
      priceUsd: swap.priceUsd || 0,
      priceBase: swap.priceBase || 0,
      pairAddress: swap.pairAddress,
    },
  })

  console.log("[saved]", swap.side, swap.symbol, swap.txHash)
}

async function processTx(txHash: string) {
  if (seen.has(txHash)) return

  seen.add(txHash)

  if (seen.size > 10000) {
    const first = seen.values().next().value
    if (first) seen.delete(first)
  }

  const tx = await getTx(txHash)
  const events = extractEvents(tx)

  await detectLaunch(events)
  await detectGraduation(events)

  const curveTrade = await parseCurveTrade(tx, events)

  if (curveTrade) {
    await saveSwap(curveTrade)
    return
  }

  const terraportTrade = await parseTerraportTrade(tx, events)

  if (terraportTrade) {
    await saveSwap(terraportTrade)
    return
  }

  console.log("[skip]", txHash)
}

function start() {
  const ws = new WebSocket(RPC_WS)

  ws.on("open", () => {
    ws.send(
      JSON.stringify({
        jsonrpc: "2.0",
        method: "subscribe",
        id: 1,
        params: {
          query: "tm.event='Tx'",
        },
      })
    )

    console.log("[terra-ws] subscribed")
  })

  ws.on("message", async raw => {
    try {
      const msg = JSON.parse(raw.toString())

      const txHash =
        msg?.result?.events?.["tx.hash"]?.[0] ||
        msg?.result?.data?.value?.TxResult?.hash

      if (!txHash) return

      await processTx(txHash)
    } catch (err) {
      console.error("[worker-error]", err)
    }
  })

  ws.on("close", () => {
    console.log("[terra-ws] closed; reconnecting")
    setTimeout(start, 3000)
  })

  ws.on("error", err => {
    console.error("[terra-ws] error", err)
    ws.close()
  })
}

start()