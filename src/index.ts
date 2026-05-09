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

const FRONTEND_URL = process.env.FRONTEND_URL?.replace(/\/$/, "")
const CRON_SECRET = process.env.CRON_SECRET
const DEFAULT_SYMBOL = process.env.PUMPIZZAB_DEFAULT_SYMBOL || "PUMP"
const LUNC_DEX_TOKEN = "0xbd31ea8212119f94a611fa969881cba3ea06fa3d"
const DEX_API = "https://api.dexscreener.com/latest/dex/tokens"

let baseUsdCache: { price: number; at: number } | null = null
const seen = new Set<string>()

function getAttr(event: any, key: string) {
  return event?.attributes?.find((a: any) => a.key === key)?.value
}

function safeNumber(value: any) {
  const n = Number(String(value ?? "0").replaceAll(",", ""))
  return Number.isFinite(n) ? n : 0
}

function fromMicro(value: any) {
  return safeNumber(value) / 1_000_000
}

async function getBaseUsd() {
  const envPrice = Number(
    process.env.PUMPIZZAB_BASE_USD ||
      process.env.LUNC_USD ||
      process.env.BASE_USD ||
      0
  )

  if (Number.isFinite(envPrice) && envPrice > 0) return envPrice

  const now = Date.now()
  if (baseUsdCache && now - baseUsdCache.at < 60_000) return baseUsdCache.price

  try {
    const res = await fetch(`${DEX_API}/${LUNC_DEX_TOKEN}`)
    const json = await res.json().catch(() => null)
    const pairs = Array.isArray(json?.pairs) ? json.pairs : []
    const best = pairs
      .filter((pair: any) => Number(pair?.priceUsd) > 0)
      .sort((a: any, b: any) => Number(b?.liquidity?.usd || 0) - Number(a?.liquidity?.usd || 0))[0]

    const price = Number(best?.priceUsd || 0)
    if (Number.isFinite(price) && price > 0) {
      baseUsdCache = { price, at: now }
      return price
    }
  } catch {}

  return baseUsdCache?.price || 0.000117
}

async function getTx(txHash: string) {
  const res = await fetch(`${LCD}/cosmos/tx/v1beta1/txs/${txHash}`)

  if (!res.ok) {
    throw new Error(`LCD fetch failed ${res.status}: ${txHash}`)
  }

  return res.json()
}

function extractEvents(tx: any) {
  const responseEvents = tx?.tx_response?.events || []
  const logEvents = (tx?.tx_response?.logs || []).flatMap(
    (log: any) => log.events || []
  )

  return [...responseEvents, ...logEvents]
}

async function getKnownToken(input: { token?: string; contract?: string; pair?: string }) {
  try {
    return prisma.pumpToken.findFirst({
      where: {
        OR: [
          input.token ? { token: input.token } : undefined,
          input.contract ? { curveContract: input.contract } : undefined,
          input.contract ? { launchContract: input.contract } : undefined,
          input.pair ? { terraportPair: input.pair } : undefined,
        ].filter(Boolean) as any,
      },
    })
  } catch {
    return null
  }
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
  if (!input.token || input.token === "unknown") return null

  try {
    return prisma.pumpToken.upsert({
      where: { token: input.token },
      update: {
        symbol: input.symbol || undefined,
        name: input.name || undefined,
        launchContract: input.launchContract || undefined,
        curveContract: input.curveContract || undefined,
        terraportPair: input.terraportPair || undefined,
        graduated: input.graduated ?? undefined,
      },
      create: {
        token: input.token,
        symbol: input.symbol || DEFAULT_SYMBOL,
        name: input.name,
        launchContract: input.launchContract,
        curveContract: input.curveContract,
        terraportPair: input.terraportPair,
        graduated: input.graduated ?? false,
      },
    })
  } catch {
    return null
  }
}

async function parseCurveTrade(tx: any, events: any[]) {
  const txHash = tx?.tx_response?.txhash
  const wasm = events.filter((e: any) => e.type === "wasm")

  const trade = wasm.find((e: any) => {
    const action = getAttr(e, "action")
    return (
      (action === "buy" || action === "sell") &&
      getAttr(e, "amount_in") &&
      getAttr(e, "amount_out")
    )
  })

  if (!trade || !txHash) return null

  const side = getAttr(trade, "action") === "sell" ? "sell" : "buy"
  const pairAddress = getAttr(trade, "_contract_address")

  const tokenTransfer = wasm.find((e: any) => {
    const action = getAttr(e, "action")
    const from = getAttr(e, "from")
    const to = getAttr(e, "to")
    return action === "transfer" && (from === pairAddress || to === pairAddress)
  })

  const token = getAttr(tokenTransfer, "_contract_address")
  if (!token) return null

  const known = await getKnownToken({ token, contract: pairAddress })
  if (!known) {
    await registerPumpToken({
      token,
      curveContract: pairAddress,
      symbol: DEFAULT_SYMBOL,
      graduated: false,
    })
  }

  const amountIn = fromMicro(getAttr(trade, "amount_in"))
  const amountOut = fromMicro(getAttr(trade, "amount_out"))
  const amountBase = side === "buy" ? amountIn : amountOut
  const amountToken = side === "buy" ? amountOut : amountIn

  if (amountBase <= 0 || amountToken <= 0) return null

  const baseUsd = await getBaseUsd()
  const priceBase = amountBase / amountToken
  const priceUsd = baseUsd > 0 ? priceBase * baseUsd : 0
  const amountUsd = baseUsd > 0 ? amountBase * baseUsd : 0

  return {
    txHash,
    token,
    symbol: known?.symbol || DEFAULT_SYMBOL,
    trader: getAttr(trade, "buyer") || getAttr(trade, "seller") || "unknown",
    side,
    amountToken,
    amountBase,
    amountUsd,
    priceUsd,
    priceBase,
    pairAddress,
  }
}

async function saveSwap(swap: any) {
  const row = await prisma.swapEvent.upsert({
    where: { txHash: swap.txHash },
    update: {},
    create: {
      txHash: swap.txHash,
      token: swap.token,
      symbol: swap.symbol || DEFAULT_SYMBOL,
      trader: swap.trader || "unknown",
      side: swap.side || "buy",
      amountToken: swap.amountToken || 0,
      amountBase: swap.amountBase || 0,
      amountUsd: swap.amountUsd || 0,
      priceUsd: swap.priceUsd || 0,
      priceBase: swap.priceBase || 0,
      pairAddress: swap.pairAddress || null,
    },
  })

  console.log("[saved]", swap.side, swap.symbol, swap.txHash)
  return row
}

async function triggerAlert() {
  if (!FRONTEND_URL || !CRON_SECRET) return

  try {
    const url = `${FRONTEND_URL}/api/pumpizzab/trigger-alert?secret=${encodeURIComponent(CRON_SECRET)}`
    const res = await fetch(url)
    console.log("[alert-trigger]", res.status, await res.text())
  } catch (err) {
    console.error("[broadcast-error]", err)
  }
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
  const swap = await parseCurveTrade(tx, events)

  if (!swap) {
    console.log("[skip]", txHash)
    return
  }

  await saveSwap(swap)
  await triggerAlert()
}

function start() {
  const ws = new WebSocket(RPC_WS)

  ws.on("open", () => {
    ws.send(
      JSON.stringify({
        jsonrpc: "2.0",
        method: "subscribe",
        id: 1,
        params: { query: "tm.event='Tx'" },
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

      if (txHash) await processTx(txHash)
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

if (process.env.MANUAL_TX) {
  processTx(process.env.MANUAL_TX).finally(() => prisma.$disconnect())
} else {
  start()
}
