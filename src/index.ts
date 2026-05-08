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

const seen = new Set<string>()

async function getTx(txHash: string) {
  const res = await fetch(`${LCD}/cosmos/tx/v1beta1/txs/${txHash}`)

  if (!res.ok) {
    throw new Error(`LCD fetch failed ${res.status}: ${txHash}`)
  }

  return res.json()
}

function getAttr(event: any, key: string) {
  return event.attributes?.find((a: any) => a.key === key)?.value
}

function parseSwap(tx: any) {
  const txHash = tx?.tx_response?.txhash
  const events = tx?.tx_response?.events || []

  const wasm = events.filter((e: any) => e.type === "wasm")

  const trade = wasm.find((e: any) => {
    const action = getAttr(e, "action")
    return action === "buy" || action === "sell"
  })

  if (!trade) return null

  const side = getAttr(trade, "action")
  const pairAddress = getAttr(trade, "_contract_address")
  const trader = getAttr(trade, "buyer") || getAttr(trade, "seller")

  const amountInRaw = Number(getAttr(trade, "amount_in") || 0)
  const amountOutRaw = Number(getAttr(trade, "amount_out") || 0)

  const transfer = wasm.find((e: any) => {
    return (
      getAttr(e, "action") === "transfer" &&
      getAttr(e, "from") === pairAddress &&
      !!getAttr(e, "to")
    )
  })

  const token =
    getAttr(transfer, "_contract_address") ||
    "unknown"

  const amountBase =
    side === "buy"
      ? amountInRaw / 1_000_000
      : amountOutRaw / 1_000_000

  const amountToken =
    side === "buy"
      ? amountOutRaw / 1_000_000
      : amountInRaw / 1_000_000

  const priceBase =
    amountToken > 0 ? amountBase / amountToken : 0

  return {
    txHash,
    token,
    symbol: "PUMP",
    trader: trader || "unknown",
    side,
    amountToken,
    amountBase,
    amountUsd: 0,
    priceUsd: 0,
    priceBase,
    pairAddress
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
  const parsed = parseSwap(tx)

  if (!parsed) {
    console.log("[skip]", txHash)
    return
  }

  await prisma.swapEvent.upsert({
    where: {
      txHash: parsed.txHash
    },
    update: {},
    create: parsed
  })

  console.log(
    "[saved]",
    parsed.side,
    parsed.symbol,
    parsed.txHash
  )

  if (process.env.FRONTEND_URL && process.env.CRON_SECRET) {
    try {
      const url =
        `${process.env.FRONTEND_URL}/api/pumpizzab/trigger-alert?secret=${process.env.CRON_SECRET}`

      const res = await fetch(url)

      console.log(
        "[alert-trigger]",
        res.status,
        await res.text()
      )
    } catch (err) {
      console.error("[broadcast-error]", err)
    }
  }
}

function start() {
  const ws = new WebSocket(RPC_WS)

  ws.on("open", () => {
    ws.send(JSON.stringify({
      jsonrpc: "2.0",
      method: "subscribe",
      id: 1,
      params: {
        query: "tm.event='Tx'"
      }
    }))

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