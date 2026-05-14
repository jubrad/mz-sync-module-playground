declare module 'wa-sqlite/dist/wa-sqlite.mjs' {
  export default function SQLiteESMFactory(): Promise<any>
}

declare module 'wa-sqlite/src/sqlite-api.js' {
  export function Factory(module: any): any
  export const SQLITE_ROW: number
  export const SQLITE_DONE: number
  export const SQLITE_OK: number
}
