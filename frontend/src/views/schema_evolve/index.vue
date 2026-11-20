<template>
  <section class="page" data-module="schema_evolve">
    <header class="page-head">
      <div>
        <h2>Schema演进管理</h2>
        <p class="page-desc">维护消息Schema的前向/后向兼容演进与字段变更控制，为Schema演进提供可验证的数据工程流程。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记演进规则</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出Schema演进清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无Schema演进数据，可先登记演进规则</td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记演进规则</h3>
        <label v-for="field in columns" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <section class="migration-panel" data-testid="schema-migration-checks">
      <header class="migration-head">
        <div>
          <h3>迁移计划检查点版本核对</h3>
          <p class="page-desc">读取流式计算生成的任务状态迁移数据计划，核对检查点字段版本与主题 Schema 版本。</p>
        </div>
        <button class="btn" type="button" data-testid="refresh-migration-checks" @click="loadMigrationChecks">刷新核对</button>
      </header>
      <p v-if="!migrationChecks.length" class="migration-empty" data-testid="migration-checks-empty">暂无迁移数据计划，可先在流式计算页生成</p>
      <table v-else class="data-table">
        <thead>
          <tr><th>计划编号</th><th>关联主题</th><th>检查点版本</th><th>Schema版本</th><th>核对结果</th><th>计划状态</th></tr>
        </thead>
        <tbody>
          <tr v-for="check in migrationChecks" :key="check.plan_id" :data-testid="`migration-check-${check.plan_id}`" :data-compatible="check.compatible">
            <td>{{ check.plan_code }}</td>
            <td>{{ check.topic_code || '—' }}</td>
            <td>{{ check.checkpoint_version || '—' }}</td>
            <td>{{ check.schema_version || '—' }}</td>
            <td>{{ check.compatible ? '一致' : '不一致' }}</td>
            <td>{{ check.plan_status || '—' }}</td>
          </tr>
        </tbody>
      </table>
    </section>

    <footer class="page-foot">
      <span>共 {{ total }} 条Schema演进记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

const ENDPOINT = '/api/schema_evolve'
const columns = ["规则编号", "关联主题", "新增字段", "删除字段", "类型变更", "默认值填充", "兼容模式", "生效版本"]
const actions = ["提交校验", "标记生效", "执行回滚"]
const statuses = ["草稿", "待校验", "已生效", "已回滚"]
const metricNames = ["演进规则总数", "待校验规则", "已生效规则"]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const migrationChecks = ref<any[]>([])
const stats = computed(() => [
  { label: metricNames[0], value: total.value },
  { label: metricNames[1], value: rows.value.filter((row) => row.status === statuses[1]).length },
  { label: metricNames[2], value: rows.value.filter((row) => row.status === statuses[statuses.length - 1]).length },
])

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value).toString()
  try {
    const response = await request(`${ENDPOINT}/export?${query}`)
    if (!response.ok) throw new Error('导出失败，请稍后重试')
    const url = URL.createObjectURL(await response.blob())
    const link = document.createElement('a')
    link.href = url
    link.download = 'schema_evolve.csv'
    link.click()
    URL.revokeObjectURL(url)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(columns.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const response = await request(ENDPOINT, {
      method: 'POST', body: JSON.stringify(createValues.value),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '登记失败')
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('Schema演进动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : 'Schema演进操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('演进规则列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : 'Schema演进列表读取失败'
  }
}

async function loadMigrationChecks() {
  try {
    const response = await request(`${ENDPOINT}/migration-checks`)
    if (!response.ok) throw new Error('迁移计划核对读取失败')
    migrationChecks.value = await response.json()
  } catch (error) {
    migrationChecks.value = []
  }
}

onMounted(() => {
  void reload()
  void loadMigrationChecks()
})
</script>
