const BASE = '/api'

async function request(method, path, data) {
  const opts = { method }
  if (data) {
    opts.headers = { 'Content-Type': 'application/json' }
    opts.body = JSON.stringify(data)
  }
  const r = await fetch(BASE + path, opts)
  if (!r.ok) {
    const err = new Error()
    err.response = { status: r.status }
    throw err
  }
  return r.json()
}

export const getTags = () => request('GET', '/tags')
export const createTag = (data) => request('POST', '/tags', data)
export const updateTag = (tag_name, data) => request('PUT', `/tags/${encodeURIComponent(tag_name)}`, data)
export const deleteTag = (tag_name) => request('DELETE', `/tags/${encodeURIComponent(tag_name)}`)

export const getSelector = (tags, search, market, industry, page, pageSize) => {
  let params = []
  if (tags && tags.length) params.push(`tags=${encodeURIComponent(tags.join(','))}`)
  if (search) params.push(`search=${encodeURIComponent(search)}`)
  if (market) params.push(`market=${encodeURIComponent(market)}`)
  if (industry) params.push(`industry=${encodeURIComponent(industry)}`)
  if (page) params.push(`page=${page}`)
  if (pageSize) params.push(`page_size=${pageSize}`)
  const q = params.length ? '?' + params.join('&') : ''
  return request('GET', `/selector${q}`)
}

export const getStockTags = (stock_code) => request('GET', `/stock-tags?stock_code=${encodeURIComponent(stock_code)}`)
export const addStockTag = (stock_code, tag_name) =>
  request('POST', `/stock-tags?stock_code=${encodeURIComponent(stock_code)}&tag_name=${encodeURIComponent(tag_name)}`)
export const removeStockTag = (stock_code, tag_name) => request('DELETE', `/stock-tags?stock_code=${encodeURIComponent(stock_code)}&tag_name=${encodeURIComponent(tag_name)}`)
