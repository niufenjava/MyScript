<template>
  <div class="page-container">
    <!-- 筛选区 -->
    <el-card class="filter-card" shadow="never">
      <div class="filter-row">
        <el-input
          v-model="searchKeyword"
          @input="onFilterChange"
          placeholder="搜索股票代码或名称..."
          clearable
          style="width: 180px"
        />
        <el-select
          v-model="selectedMarket"
          @change="onFilterChange"
          placeholder="市场"
          clearable
          style="width: 160px"
        >
          <el-option value="上交所" label="上交所" />
          <el-option value="深交所主板" label="深交所主板" />
          <el-option value="上交所科创板" label="上交所科创板" />
          <el-option value="深交所创业板" label="深交所创业板" />
        </el-select>
        <el-input
          v-model="selectedIndustry"
          @input="onFilterChange"
          placeholder="行业关键词..."
          clearable
          style="width: 150px"
        />
        <el-button @click="reset">重置</el-button>
      </div>
      <!-- 标签筛选 -->
      <div class="tag-row">
        <el-tag
          v-for="tag in tags"
          :key="tag.tag_name"
          class="filter-tag"
          :color="selectedTags.includes(tag.tag_name) ? tag.color : 'transparent'"
          :style="{
            color: selectedTags.includes(tag.tag_name) ? '#fff' : tag.color,
            borderColor: tag.color,
          }"
          @click="toggleTag(tag.tag_name)"
        >
          {{ tag.tag_name }}
        </el-tag>
      </div>
    </el-card>

    <!-- 股票表格 -->
    <el-card class="table-card" shadow="never">
      <el-table :data="stocks" v-loading="loading" stripe style="width: 100%">
        <el-table-column prop="stock_code" label="代码" width="110" />
        <el-table-column prop="stock_name" label="名称" width="110" />
        <el-table-column prop="market" label="市场" width="130" />
        <el-table-column prop="industry" label="行业" width="130" />
        <el-table-column label="最新价" width="100" align="right">
          <template #default="{ row }">
            {{ row.latest_price != null ? row.latest_price.toFixed(2) : '-' }}
          </template>
        </el-table-column>
        <el-table-column label="总市值" width="110" align="right">
          <template #default="{ row }">
            {{ row.total_market_cap != null ? row.total_market_cap.toFixed(2) + '亿' : '-' }}
          </template>
        </el-table-column>
        <el-table-column label="市盈率TTM" width="110" align="right">
          <template #default="{ row }">
            {{ row.pe_ttm != null ? row.pe_ttm.toFixed(2) : '-' }}
          </template>
        </el-table-column>
        <el-table-column label="ROE" width="80" align="right">
          <template #default="{ row }">
            {{ row.roe != null ? row.roe.toFixed(2) + '%' : '-' }}
          </template>
        </el-table-column>
        <el-table-column label="标签" min-width="160">
          <template #default="{ row }">
            <div class="tag-list">
              <el-tag
                v-for="tag in row.tags"
                :key="tag"
                size="small"
                closable
                :color="tagColorMap[tag] ?? '#909399'"
                style="color:#fff"
                @close="removeTag(row.stock_code, tag)"
              >
                {{ tag }}
              </el-tag>
            </div>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="90" align="center">
          <template #default="{ row }">
            <el-button type="primary" link @click="openTagModal(row)">+ 打标签</el-button>
          </template>
        </el-table-column>
        <template #empty>
          <el-empty description="暂无匹配的股票" />
        </template>
      </el-table>

      <div class="pagination">
        <el-pagination
          v-model:current-page="currentPage"
          :page-size="PAGE_SIZE"
          :total="totalCount"
          layout="total, prev, pager, next"
          @current-change="fetchStocks"
        />
      </div>
    </el-card>

    <!-- 打标签弹窗 -->
    <el-dialog v-model="showTagModal" :title="`给 ${modalStock?.stock_name} 打标签`" width="380px">
      <div class="tag-dialog-area">
        <el-tag
          v-for="tag in allTags"
          :key="tag.tag_name"
          class="dialog-tag"
          :color="(modalStock?.tags ?? []).includes(tag.tag_name) ? tag.color : 'transparent'"
          :style="{
            color: (modalStock?.tags ?? []).includes(tag.tag_name) ? '#fff' : tag.color,
            borderColor: tag.color,
          }"
          @click="addTagToStock(tag.tag_name)"
        >
          {{ tag.tag_name }}
        </el-tag>
      </div>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import { getTags, getSelector, addStockTag, removeStockTag } from '@/api.js'

const PAGE_SIZE = 30

const tags = ref<any[]>([])
const stocks = ref<any[]>([])
const selectedTags = ref<string[]>([])
const searchKeyword = ref('')
const selectedMarket = ref('')
const selectedIndustry = ref('')
const tagColorMap = ref<Record<string, string>>({})
const loading = ref(false)
const currentPage = ref(1)
const totalCount = ref(0)
const showTagModal = ref(false)
const modalStock = ref<any>(null)
const allTags = ref<any[]>([])

async function fetchTags() {
  tags.value = await getTags()
  allTags.value = [...tags.value]
  for (const t of tags.value) tagColorMap.value[t.tag_name] = t.color
}

async function fetchStocks() {
  loading.value = true
  try {
    const res = await getSelector(
      selectedTags.value.length ? selectedTags.value : undefined,
      searchKeyword.value || undefined,
      selectedMarket.value || undefined,
      selectedIndustry.value || undefined,
      currentPage.value,
      PAGE_SIZE,
    )
    stocks.value = res.data ?? []
    totalCount.value = res.total ?? 0
  } finally {
    loading.value = false
  }
}

function onFilterChange() {
  currentPage.value = 1
  fetchStocks()
}

function toggleTag(tagName: string) {
  const idx = selectedTags.value.indexOf(tagName)
  if (idx >= 0) selectedTags.value.splice(idx, 1)
  else selectedTags.value.push(tagName)
  currentPage.value = 1
  fetchStocks()
}

function reset() {
  selectedTags.value = []
  searchKeyword.value = ''
  selectedMarket.value = ''
  selectedIndustry.value = ''
  currentPage.value = 1
  fetchStocks()
}

async function openTagModal(stock: any) {
  modalStock.value = stock
  showTagModal.value = true
}

function closeTagModal() {
  showTagModal.value = false
  modalStock.value = null
}

async function addTagToStock(tagName: string) {
  if (!modalStock.value || (modalStock.value.tags ?? []).includes(tagName)) return
  try {
    await addStockTag(modalStock.value.stock_code, tagName)
    closeTagModal()
    await fetchStocks()
    ElMessage.success('标签已添加')
  } catch {
    ElMessage.error('添加标签失败')
  }
}

async function removeTag(stockCode: string, tagName: string) {
  try {
    await removeStockTag(stockCode, tagName)
    await fetchStocks()
  } catch {
    ElMessage.error('移除标签失败')
  }
}

onMounted(async () => {
  await fetchTags()
  await fetchStocks()
})
</script>

<style scoped>
.page-container {
  padding: 24px;
  max-width: 1200px;
  margin: 0 auto;
}
.filter-card {
  margin-bottom: 16px;
  border-radius: 8px;
}
.filter-row {
  display: flex;
  gap: 12px;
  align-items: center;
  flex-wrap: wrap;
  margin-bottom: 12px;
}
.tag-row {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}
.filter-tag {
  cursor: pointer;
  border-width: 1.5px;
}
.table-card {
  border-radius: 8px;
}
.tag-list {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
}
.pagination {
  display: flex;
  justify-content: center;
  padding: 16px 0 0;
}
.tag-dialog-area {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  max-height: 280px;
  overflow-y: auto;
}
.dialog-tag {
  cursor: pointer;
  border-width: 1.5px;
}
</style>
