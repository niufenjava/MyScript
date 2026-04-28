<template>
  <div class="page-container">
    <!-- 新增区域 -->
    <el-card class="add-card" shadow="never">
      <div class="add-row">
        <el-input
          v-model="newTagName"
          placeholder="输入标签名称，按回车添加"
          @keyup.enter="addTag"
          style="width: 220px"
          clearable
        />
        <el-color-picker v-model="newTagColor" size="large" />
        <el-button type="primary" @click="addTag">添加标签</el-button>
      </div>
    </el-card>

    <!-- 标签列表 -->
    <div v-if="tags.length" class="tags-grid">
      <div
        v-for="tag in tags"
        :key="tag.tag_name"
        class="tag-item"
        :style="{ borderLeftColor: tag.color }"
      >
        <div class="tag-left">
          <span class="tag-dot" :style="{ backgroundColor: tag.color }"></span>
          <span class="tag-name">{{ tag.tag_name }}</span>
        </div>
        <div class="tag-actions">
          <el-color-picker
            :model-value="tag.color"
            @change="(val: string) => updateColor(tag.tag_name, val)"
            size="small"
          />
          <el-button type="danger" size="small" plain @click="removeTag(tag.tag_name)">
            删除
          </el-button>
        </div>
      </div>
    </div>

    <el-empty v-else description="暂无标签" />
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import { getTags, createTag, updateTag, deleteTag } from '@/api.js'

const tags = ref<any[]>([])
const newTagName = ref('')
const newTagColor = ref('#409EFF')

async function fetchTags() {
  tags.value = await getTags()
}

async function addTag() {
  const name = newTagName.value.trim()
  if (!name) return
  try {
    await createTag({ tag_name: name, color: newTagColor.value })
    newTagName.value = ''
    await fetchTags()
    ElMessage.success('添加成功')
  } catch (e: any) {
    if (e.response?.status === 409) {
      ElMessage.warning('标签已存在')
    } else {
      ElMessage.error('添加失败')
    }
  }
}

async function removeTag(tag_name: string) {
  try {
    await deleteTag(tag_name)
    await fetchTags()
    ElMessage.success('删除成功')
  } catch (e: any) {
    if (e.response?.status === 409) {
      ElMessage.error('该标签已被使用，无法删除')
    } else {
      ElMessage.error('删除失败')
    }
  }
}

async function updateColor(tag_name: string, color: string) {
  if (!color) return
  try {
    await updateTag(tag_name, { color })
    await fetchTags()
  } catch {
    ElMessage.error('修改颜色失败')
  }
}

onMounted(fetchTags)
</script>

<style scoped>
.page-container {
  padding: 24px;
  max-width: 900px;
  margin: 0 auto;
}
.add-card {
  margin-bottom: 24px;
  border-radius: 8px;
}
.add-row {
  display: flex;
  gap: 12px;
  align-items: center;
}
.tags-grid {
  display: flex;
  flex-direction: column;
  gap: 8px;
}
.tag-item {
  display: flex;
  align-items: center;
  justify-content: space-between;
  background: #fff;
  border-radius: 8px;
  padding: 12px 16px;
  border-left: 4px solid;
  transition: box-shadow 0.2s;
}
.tag-item:hover {
  box-shadow: 0 2px 12px rgba(0,0,0,0.08);
}
.tag-left {
  display: flex;
  align-items: center;
  gap: 10px;
}
.tag-dot {
  width: 12px;
  height: 12px;
  border-radius: 50%;
  flex-shrink: 0;
}
.tag-name {
  font-size: 15px;
  color: #303133;
}
.tag-actions {
  display: flex;
  align-items: center;
  gap: 8px;
}
</style>
