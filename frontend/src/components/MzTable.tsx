import { useMemo, useState } from 'react'
import {
  useReactTable,
  getCoreRowModel,
  getSortedRowModel,
  getFilteredRowModel,
  flexRender,
  createColumnHelper,
  type SortingState,
} from '@tanstack/react-table'

interface MzTableProps {
  rows: Record<string, unknown>[]
  columns: string[]
  onRowClick?: (row: Record<string, unknown>) => void
}

export function MzTable({ rows, columns, onRowClick }: MzTableProps) {
  const [sorting, setSorting] = useState<SortingState>([])
  const [globalFilter, setGlobalFilter] = useState('')

  const colHelper = createColumnHelper<Record<string, unknown>>()

  const tableCols = useMemo(() => {
    const allCols = ['_key', ...columns]
    return allCols.map((col) =>
      colHelper.accessor((row) => row[col], {
        id: col,
        header: col === '_key' ? 'Key' : col,
        cell: (info) => {
          const v = info.getValue()
          if (v === null || v === undefined) return '—'
          if (typeof v === 'object') return JSON.stringify(v)
          return String(v)
        },
      }),
    )
  }, [columns])

  const table = useReactTable({
    data: rows,
    columns: tableCols,
    state: { sorting, globalFilter },
    onSortingChange: setSorting,
    onGlobalFilterChange: setGlobalFilter,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
  })

  return (
    <div>
      <input
        value={globalFilter}
        onChange={(e) => setGlobalFilter(e.target.value)}
        placeholder="Search..."
        style={{
          marginBottom: 12,
          padding: '8px 12px',
          width: '100%',
          maxWidth: 400,
          border: '1px solid #dee2e6',
          borderRadius: 6,
          fontSize: 14,
        }}
      />
      <div style={{ overflow: 'auto', maxHeight: 500, border: '1px solid #dee2e6', borderRadius: 8 }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
          <thead style={{ position: 'sticky', top: 0, background: '#fff', zIndex: 1 }}>
            {table.getHeaderGroups().map((hg) => (
              <tr key={hg.id}>
                {hg.headers.map((header) => (
                  <th
                    key={header.id}
                    onClick={header.column.getToggleSortingHandler()}
                    style={{
                      padding: '8px 12px',
                      borderBottom: '2px solid #dee2e6',
                      textAlign: 'left',
                      cursor: 'pointer',
                      userSelect: 'none',
                      fontSize: 11,
                      textTransform: 'uppercase',
                      letterSpacing: '0.5px',
                      color: '#868e96',
                      fontWeight: 600,
                    }}
                  >
                    {flexRender(header.column.columnDef.header, header.getContext())}
                    {{ asc: ' ▲', desc: ' ▼' }[header.column.getIsSorted() as string] ?? ''}
                  </th>
                ))}
              </tr>
            ))}
          </thead>
          <tbody>
            {table.getRowModel().rows.map((row) => (
              <tr
                key={row.id}
                onClick={() => onRowClick?.(row.original)}
                style={{ cursor: onRowClick ? 'pointer' : 'default' }}
                onMouseEnter={(e) => (e.currentTarget.style.background = '#f1f3f5')}
                onMouseLeave={(e) => (e.currentTarget.style.background = '')}
              >
                {row.getVisibleCells().map((cell) => (
                  <td
                    key={cell.id}
                    style={{
                      padding: '6px 12px',
                      borderBottom: '1px solid #dee2e6',
                      maxWidth: 300,
                      overflow: 'hidden',
                      textOverflow: 'ellipsis',
                      whiteSpace: 'nowrap',
                      fontFamily: cell.column.id === '_key' ? 'monospace' : 'inherit',
                      color: cell.column.id === '_key' ? '#1971c2' : '#212529',
                      fontWeight: cell.column.id === '_key' ? 600 : 400,
                    }}
                  >
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </td>
                ))}
              </tr>
            ))}
            {table.getRowModel().rows.length === 0 && (
              <tr>
                <td
                  colSpan={tableCols.length}
                  style={{ padding: 24, textAlign: 'center', color: '#868e96' }}
                >
                  {globalFilter ? 'No matching rows.' : 'No data yet.'}
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
      <div style={{ marginTop: 8, fontSize: 12, color: '#868e96' }}>
        {table.getFilteredRowModel().rows.length} row{table.getFilteredRowModel().rows.length !== 1 ? 's' : ''}
        {globalFilter && ` (filtered from ${rows.length})`}
      </div>
    </div>
  )
}
