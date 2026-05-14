import { createContext, useContext, useMemo, type ReactNode } from 'react'
import { MzStore } from './mz-store'
import type { MzStoreOptions } from './types'

interface MzContextValue {
  baseUrl: string
  store: MzStore
}

const MzContext = createContext<MzContextValue | null>(null)

export function MzProvider({
  baseUrl,
  children,
  opts,
}: {
  baseUrl: string
  children: ReactNode
  opts?: MzStoreOptions
}) {
  const store = useMemo(() => new MzStore(baseUrl, opts), [baseUrl])

  const value = useMemo(() => ({ baseUrl, store }), [baseUrl, store])

  return <MzContext.Provider value={value}>{children}</MzContext.Provider>
}

export function useMzContext(): MzContextValue {
  const ctx = useContext(MzContext)
  if (!ctx) throw new Error('useMzContext must be used within <MzProvider>')
  return ctx
}
