<template>
  <section class="page" data-module="flink_job">
    <header class="page-head">
      <div><h2>流式计算</h2><p class="page-desc">维护Flink流式任务的SQL逻辑、状态与检查点配置，为流式计算提供可验证的数据工程流程。</p></div>
      <div class="page-actions">
        <button class="btn primary" type="button" data-testid="open-create" @click="openCreate">登记流式任务</button>
        <button class="btn" type="button" data-testid="export" @click="exportRows">导出作业清单</button>
      </div>
    </header>
    <div class="filter-bar">
      <label><span>开始日期</span><input type="date" v-model="dateFrom" /></label>
      <label><span>结束日期</span><input type="date" v-model="dateTo" /></label>
      <button class="btn" type="button" @click="reload">查询</button>
    </div>
    <div class="timeline-wrap">
      <div v-for="item in timelineItems" :key="item.id" class="timeline-row">
        <span class="timeline-label">{{ item.name || item.label }}</span>
        <div class="timeline-track">
          <div class="timeline-bar" :class="`status-${item.status}`" :style="{ left: barLeft(item) + '%', width: barWidth(item) + '%' }" :data-testid="`bar-${item.id}`" :title="`${item.start} ~ ${item.end}`" @click="selectItem(item)">&nbsp;</div>
        </div>
        <span style="font-size:12px;color:var(--muted);margin-left:8px;">{{ item.start }} ~ {{ item.end }}</span>
      </div>
      <p v-if="!timelineItems.length" style="text-align:center;color:var(--muted)">暂无作业记录</p>
    </div>
    <div v-if="selectedItem" class="timeline-log" data-testid="timeline-log">
      <strong>{{ selectedItem.name || selectedItem.label }}</strong> — 状态：{{ selectedItem.status }}
      <div style="margin-top:4px;">{{ selectedItem.logs || '暂无日志' }}</div>
    </div>

    <section class="migration-panel" data-testid="migration-panel">
      <header class="migration-head">
        <div>
          <h3>任务状态迁移数据计划</h3>
          <p class="page-desc">为选中的订单聚合任务生成迁移数据计划，计划同时写入检查点版本与可恢复输出表。</p>
        </div>
        <div class="page-actions">
          <button class="btn primary" type="button" data-testid="generate-plan" :disabled="!selectedItem || generating" @click="generatePlan(false)">
            {{ generating ? '生成中…' : '生成迁移计划' }}
          </button>
          <button class="btn" type="button" data-testid="resume-plan" :disabled="!plan || generating" @click="generatePlan(true)">中断后续做</button>
        </div>
      </header>
      <p v-if="!selectedItem" class="migration-empty" data-testid="migration-hint">请先在时间线中选择一个流式任务</p>
      <p v-else-if="!plan" class="migration-empty" data-testid="migration-hint">任务 {{ selectedItem.name || selectedItem.label }} 尚未生成迁移数据计划</p>
      <dl v-if="plan" class="migration-detail" data-testid="migration-detail">
        <div><dt>计划编号</dt><dd>{{ plan.plan_code }}</dd></div>
        <div><dt>计划状态</dt><dd data-testid="plan-status">{{ plan.status }}</dd></div>
        <div><dt>检查点版本</dt><dd data-testid="plan-checkpoint">v{{ plan.checkpoint_version }}</dd></div>
        <div><dt>输出表</dt><dd>{{ plan.output_table }}</dd></div>
        <div><dt>可恢复输出表</dt><dd data-testid="plan-output">{{ formatRecoverable(plan.recoverable_outputs) }}</dd></div>
        <div><dt>可恢复点</dt><dd data-testid="plan-recoverable">检查点 v{{ plan.recoverable_point.checkpoint_version }} · {{ plan.recoverable_point.output_table }} {{ plan.recoverable_point.output_table_version || '—' }}</dd></div>
        <div class="migration-message"><dt>说明</dt><dd data-testid="plan-message">{{ plan.message }}</dd></div>
      </dl>
      <div v-if="plan" class="migration-batches">
        <span class="migration-batches-label">批次：</span>
        <span
          v-for="batch in plan.batches"
          :key="batch.batch_no"
          class="migration-batch"
          :class="{ done: batch.state === '已完成' }"
          :data-testid="`batch-${batch.batch_no}`"
        >#{{ batch.batch_no }} {{ batch.state }}</span>
      </div>
    </section>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate"><h3>登记流式任务</h3>
        <label v-for="field in columns.slice(0,3)" :key="field" class="create-field"><span>{{ field }}</span><input v-model="createValues[field]" :placeholder="`请输入${field}`" required /></label>
        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px">
          <button class="btn" type="button" @click="showCreate=false">取消</button>
          <button class="btn primary" type="submit" data-testid="submit-create">提交</button>
        </div>
      </form>
    </div>
  </section>
</template>
<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { request } from '@/api/client'
const ENDPOINT = '/api/flink_job'
const MIGRATION_ENDPOINT = '/api/task_migration_plan'
const columns = ["任务编号", "消费主题", "转换逻辑", "输出表名", "并行度", "检查点间隔", "状态后端", "水位策略"]
const actions = ["提交校验", "启动作业", "停止作业"]
const timelineItems = ref<any[]>([])
const selectedItem = ref<any>(null)
const plan = ref<any>(null)
const generating = ref(false)
const dateFrom = ref(''); const dateTo = ref('')
const errorMessage = ref(''); const showCreate = ref(false)
const createValues = ref<Record<string,string>>({})
const allItems = ref<any[]>([])
function barLeft(item: any) { const all = allItems.value.filter(i => i.start && i.end); if(!all.length) return 0; const min = Math.min(...all.map(i => new Date(i.start).getTime())); const max = Math.max(...all.map(i => new Date(i.end).getTime())); const range = max - min || 86400000; return ((new Date(item.start).getTime() - min) / range) * 100 }
function barWidth(item: any) { const all = allItems.value.filter(i => i.start && i.end); if(!all.length) return 20; const min = Math.min(...all.map(i => new Date(i.start).getTime())); const max = Math.max(...all.map(i => new Date(i.end).getTime())); const range = max - min || 86400000; return Math.max(5, ((new Date(item.end).getTime() - new Date(item.start).getTime()) / range) * 100) }
function selectItem(item: any) {
  selectedItem.value = selectedItem.value?.id === item.id ? null : item
  plan.value = null
  if (selectedItem.value) void loadPlan()
}
function formatRecoverable(outputs: any[]) {
  return (outputs || []).map((o: any) => `${o.table} ${o.version || '—'}`).join('、')
}
async function loadPlan() {
  if (!selectedItem.value) return
  try {
    const res = await request(`${MIGRATION_ENDPOINT}/job/${selectedItem.value.id}`)
    if (!res.ok) { plan.value = null; return }
    const payload = await res.json()
    plan.value = payload.exists ? payload.plan : null
  } catch (e) { plan.value = null }
}
async function generatePlan(resume: boolean) {
  if (!selectedItem.value) return
  generating.value = true
  errorMessage.value = ''
  try {
    const res = await request(`${MIGRATION_ENDPOINT}/job/${selectedItem.value.id}/generate?resume=${resume}`, { method: 'POST' })
    const payload = await res.json()
    if (!res.ok || !payload.ok) throw new Error(payload.message || '生成迁移计划失败')
    plan.value = payload.entry
  } catch (e: any) {
    errorMessage.value = e.message
  } finally {
    generating.value = false
  }
}
async function reload() {
  try { const res = await request(ENDPOINT); const data = await res.json(); allItems.value = data.items || []; applyFilter() } catch(e) { errorMessage.value = '加载失败' }
}
function applyFilter() {
  let items = allItems.value
  if(dateFrom.value) items = items.filter((i: any) => i.start >= dateFrom.value)
  if(dateTo.value) items = items.filter((i: any) => i.end <= dateTo.value)
  timelineItems.value = items; selectedItem.value = null; plan.value = null
}
function openCreate() { createValues.value = {}; showCreate.value = true }
async function submitCreate() {
  try { const res = await request(ENDPOINT, { method: 'POST', body: JSON.stringify({ values: createValues.value }) }); if(!res.ok) throw new Error('创建失败'); showCreate.value = false; await reload() } catch(e: any) { errorMessage.value = e.message }
}
function exportRows() { window.open(`${ENDPOINT}/export`, '_blank') }
onMounted(reload)
</script>
