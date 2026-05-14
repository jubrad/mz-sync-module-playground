import { createContext, useContext, type ReactNode } from 'react'
import type { MzSubscribeClient } from '@mz-web/subscribe'
import type { MzSqliteSink } from '@mz-web/sink-sqlite'

/** Context value provided by MzProvider. */
export interface MzContextValue {
  /** Base URL of the mz-http server (e.g. "http://localhost:8000"). */
  baseUrl: string
  /** The subscribe client managing view connections. */
  client: MzSubscribeClient
  /** Optional SQLite sink for local query support. */
  sqliteSink?: MzSqliteSink
}

const MzContext = createContext<MzContextValue | null>(null)

export interface MzProviderProps extends MzContextValue {
  children: ReactNode
}

/**
 * Provides the Materialize subscribe context to the React tree.
 *
 * All mz-web hooks must be used within an MzProvider.
 */
export function MzProvider({ children, ...value }: MzProviderProps) {
  return <MzContext.Provider value={value}>{children}</MzContext.Provider>
}

/**
 * Access the Materialize context.
 * Throws if called outside an MzProvider.
 */
export function useMzContext(): MzContextValue {
  const ctx = useContext(MzContext)
  if (!ctx) {
    throw new Error('useMzContext must be used within an <MzProvider>')
  }
  return ctx
}
