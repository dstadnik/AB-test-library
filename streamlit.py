import streamlit as st
import yaml
from datetime import date
import os

CONFIG_FILE = "experiments_config.yaml"
METRICS_PRESETS_FILE = "metrics_presets.yaml"

AVAILABLE_METRICS = [
    "discounts_sum", "discount_sum_w_nds", "launch_flg", "catalog_main_flg", "catalog_listing_flg",
    "search_main_flg", "search_result_flg", "product_screen_flg", "product_screen_to_cart_flg",
    "listing_to_cart_flg", "listing_to_item_flg", "search_screen_to_cart_flg", "cart_visit_flg",
    "cart_2_checkout_flg", "checkout_visit_flg", "pay_button_pushed_flg", "purhase_flg", "first_order_date",
    "orders_cnt", "items_cnt", "gmv", "cm2_amt", "net_revenue", "total_discount", "total_discount_wo_nds",
    "bonus_add", "bonus_written", "promo_discount", "gmv_add_bonuses", "net_revenue_add_bonuses", "cm",
    "gm", "pcpo", "lcpo", "orders_rec_cnt", "items_rec_cnt", "gmv_rec", "cm2_amt_rec", "net_revenue_rec",
    "rec_views_flag", "rec_a2c_flg", "rec_pdp_flg", "crb_without_pdp", "crb_with_pdp", "aic_num_pdp",
    "aic_num_wo_pdp", "aic_denum_pdp", "aic_denum_wo_pdp", "empty_search", "non_empty_search",
    "total_searches", "search_revenue", "search_order_cnt", "search_good_amount", "search_with_order_amount",
    "catalog_revenue", "catalog_order_cnt", "catalog_good_amount", "catalog_with_order_amount"
]

AGGREGATION_FUNCTIONS = [
    "sum", "avg", "max", "min", "maxIf", "count", "countIf"
]

# --- Загрузка предустановленных метрик ---
def load_presets():
    try:
        with open(METRICS_PRESETS_FILE, "r") as f:
            presets = yaml.safe_load(f)["metrics_presets"]
    except FileNotFoundError:
        presets = []
    return presets

metrics_presets = load_presets()


# --- Сохранение новой предустановленной метрики ---
def save_new_preset(new_preset):
    presets = load_presets()
    presets.append(new_preset)
    with open(METRICS_PRESETS_FILE, "w") as f:
        yaml.dump({"metrics_presets": presets}, f, sort_keys=False, allow_unicode=True)



def parse_expression(expr: str) -> str:
    if "(" in expr and ")" in expr:
        return expr
    try:
        field, agg = expr.split("-")
        return f"{agg.upper()}({field})"
    except:
        return expr

# --- Функция генерации SQL для всех метрик из эксперимента

def generate_sql_queries_for_metrics(experiment: dict, source_table: str) -> list:
    from datetime import datetime
    control_group = experiment["control_group_id"]
    test_group = experiment["test_group_id"]
    start_date = experiment["start_date"]
    end_date = experiment["end_date"]

    global_where_filters = experiment.get("filters", {}).get("where", [])
    having_filters = experiment.get("filters", {}).get("having", [])
    metrics = experiment["metrics"]

    sql_queries = []

    for m in metrics:
        metric_alias = m["name"]
        metric_type = m["type"]
        
        # Комбинируем глобальные WHERE фильтры с индивидуальными фильтрами метрики
        where_clauses = [
            f"event_date BETWEEN '{start_date}' AND '{end_date}'",
            f"(has(ab, '{control_group}') OR has(ab, '{test_group}'))"
        ]
        
        # Добавляем глобальные WHERE фильтры
        for f in global_where_filters:
            if f["operator"] == "IN":
                val = ", ".join(f"'{v}'" for v in f["value"])
                where_clauses.append(f"{f['field']} IN ({val})")
            else:
                where_clauses.append(f"{f['field']} {f['operator']} '{f['value']}'")
        
        # Добавляем индивидуальные WHERE фильтры метрики
        metric_where_filters = m.get("where_filters", [])
        for f in metric_where_filters:
            if f["operator"] == "IN":
                val = ", ".join(f"'{v}'" for v in f["value"])
                where_clauses.append(f"{f['field']} IN ({val})")
            else:
                where_clauses.append(f"{f['field']} {f['operator']} '{f['value']}'")

        group_by = "magnit_id, group_label"
        
        base_fields = [
            f"'{experiment['experiment_name']}' AS exp_name",
            "magnit_id",
            f"CASE\n    WHEN has(ab, '{control_group}') THEN 'control'\n    WHEN has(ab, '{test_group}') THEN 'test'\nEND AS group_label",
            f"'{metric_type}' AS metric_type",
            f"'{metric_alias}' AS metric_name"
        ]

        if metric_type == "basic":
            metric_expr = f"{m['expression']} AS numerator"
            denominator_expr = "1 AS denominator"
        elif metric_type == "ratio":
            numerator_expr = m["numerator"]
            denominator_expr = m["denominator"]
            metric_expr = f"{numerator_expr} AS numerator"
            denominator_expr = f"{denominator_expr} AS denominator"
        else:
            continue

        having_block = ""
        if having_filters:
            having_block = "HAVING " + " AND ".join(h["expression"] for h in having_filters)

        select_clause = ",\n    ".join(base_fields + [metric_expr, denominator_expr])

        query = (
            f"SELECT\n    {select_clause}\n"
            f"FROM {source_table}\n"
            f"WHERE {' AND '.join(where_clauses)}\n"
            f"GROUP BY {group_by}\n"
            f"{having_block}"
        )

        sql_queries.append((metric_alias, query.strip()))

    return sql_queries

st.title("📊 Добавление и управление A/B-тестами")

# Инициализация session_state
for key in ["where_filters", "having_filters", "metrics"]:
    if key not in st.session_state:
        st.session_state[key] = []

if "editing_experiment" not in st.session_state:
    st.session_state.editing_experiment = None

try:
    with open(CONFIG_FILE, "r") as f:
        config_data = yaml.safe_load(f) or {"experiments": []}
except FileNotFoundError:
    config_data = {"experiments": []}

existing_names = [exp["experiment_name"] for exp in config_data["experiments"]]

st.write("## ✏️ Создание или редактирование эксперимента")
selected_exp = st.selectbox("Выбери эксперимент для редактирования (или оставь пустым для нового)", [""] + existing_names)

# Обработка смены эксперимента
if selected_exp != st.session_state.get("current_selected_exp", ""):
    st.session_state.current_selected_exp = selected_exp
    if selected_exp:
        existing_exp = next(exp for exp in config_data["experiments"] if exp["experiment_name"] == selected_exp)
        st.session_state.metrics = existing_exp.get("metrics", [])
        st.session_state.where_filters = existing_exp.get("filters", {}).get("where", [])
        st.session_state.having_filters = existing_exp.get("filters", {}).get("having", [])
        st.session_state.editing_experiment = selected_exp
    else:
        st.session_state.metrics = []
        st.session_state.where_filters = []
        st.session_state.having_filters = []
        st.session_state.editing_experiment = None

exp_name = st.text_input("Название эксперимента", value=selected_exp if selected_exp else "")

# Получение значений для полей из существующего эксперимента
if exp_name in existing_names and selected_exp:
    st.warning("Эксперимент с таким именем уже существует. Будет перезаписан.")
    existing_exp = next(exp for exp in config_data["experiments"] if exp["experiment_name"] == exp_name)
    
    control_id = st.text_input("Control group ID", value=existing_exp.get("control_group_id", ""))
    test_id = st.text_input("Test group ID", value=existing_exp.get("test_group_id", ""))
    start = st.date_input("Дата начала", value=date.fromisoformat(existing_exp.get("start_date", date.today().isoformat())))
    end = st.date_input("Дата окончания", value=date.fromisoformat(existing_exp.get("end_date", date.today().isoformat())))
else:
    control_id = st.text_input("Control group ID")
    test_id = st.text_input("Test group ID")
    start = st.date_input("Дата начала", value=date.today())
    end = st.date_input("Дата окончания", value=date.today())

st.write("## 🎯 Добавление метрик")


# --- Выбор и добавление предустановленных метрик ---
st.write("### 📌Выбрать метрику из предустановленных")
selected_preset = st.selectbox("Предустановленные метрики", [""] + [p["name"] for p in metrics_presets], key="preset_select")
if selected_preset and st.button(f"➕ Добавить '{selected_preset}'"):
    preset = next(p for p in metrics_presets if p["name"] == selected_preset)
    if preset not in st.session_state.metrics:
        st.session_state.metrics.append(preset.copy())
        st.success(f"✅ Метрика '{selected_preset}' добавлена")
        st.rerun()



st.write("### ➕Или добавить метрику вручную")
col1, col2, col3 = st.columns(3)
with col1:
    metric = st.selectbox("Метрика", AVAILABLE_METRICS, key="metric_name")
with col2:
    agg = st.selectbox("Агрегация", AGGREGATION_FUNCTIONS, key="metric_agg")
with col3:
    label = st.text_input("Название метрики", key="metric_label")

agg_condition = ""
agg_then = ""

if agg in ["maxIf", "countIf"]:
    agg_condition = st.text_input("Условие (например: catalog_main_flg > 0)", key="agg_if_condition")
    agg_then = st.text_input("Значение если условие выполнено (например: 1)", key="agg_if_then")

# Добавляем возможность указать индивидуальные WHERE фильтры для метрики
st.write("#### 🔍 Индивидуальные WHERE фильтры для этой метрики (опционально)")
st.write("*Например, для ARPPU можно добавить фильтр: gmv > 0*")

metric_where_filters = []
if "temp_metric_where_filters" not in st.session_state:
    st.session_state.temp_metric_where_filters = []

col1, col2, col3, col4 = st.columns(4)
with col1:
    mwf_field = st.text_input("Поле", key="mwf_field")
with col2:
    mwf_op = st.selectbox("Оператор", ["=", "!=", "IN", ">=", "<=", ">", "<"], key="mwf_op")
with col3:
    mwf_value = st.text_input("Значение", key="mwf_value")
with col4:
    if st.button("➕ Добавить фильтр", key="add_metric_filter"):
        if mwf_field and mwf_op and mwf_value:
            val = [v.strip() for v in mwf_value.split(",")] if mwf_op == "IN" else mwf_value.strip()
            st.session_state.temp_metric_where_filters.append({
                "field": mwf_field,
                "operator": mwf_op,
                "value": val
            })
            st.rerun()

if st.session_state.temp_metric_where_filters:
    st.write("**Текущие фильтры для метрики:**")
    for i, f in enumerate(st.session_state.temp_metric_where_filters):
        col1, col2 = st.columns([5, 1])
        with col1:
            st.markdown(f"- `{f['field']} {f['operator']} {f['value']}`")
        with col2:
            if st.button("❌", key=f"delete_temp_mwf_{i}"):
                st.session_state.temp_metric_where_filters.pop(i)
                st.rerun()

if st.button("➕ Добавить базовую метрику"):
    expression = f"{agg}({agg_then}, {agg_condition})" if agg in ("maxIf", "countIf") and agg_condition else f"{agg}({metric})"
    label_final = label if label else expression
    new_metric = {
        "name": label_final, 
        "type": "basic", 
        "expression": expression,
        "where_filters": st.session_state.temp_metric_where_filters.copy()
    }
    if new_metric not in st.session_state.metrics:
        st.session_state.metrics.append(new_metric)
        st.session_state.temp_metric_where_filters = []  # Очищаем временные фильтры
        st.success(f"✅ Метрика {label_final} добавлена")
        st.rerun()


st.write("### ➗ Добавить ratio-метрику")
col1, col2 = st.columns(2)
with col1:
    num_metric = st.selectbox("Числитель метрика", AVAILABLE_METRICS, key="num_metric")
    num_agg = st.selectbox("Числитель агрегация", AGGREGATION_FUNCTIONS, key="num_agg")
    num_cond = st.text_input("Числитель условие (если maxIf/countIf)", key="num_cond")
    num_then = st.text_input("Числитель значение если условие верно", key="num_then")
with col2:
    denom_metric = st.selectbox("Знаменатель метрика", AVAILABLE_METRICS, key="denom_metric")
    denom_agg = st.selectbox("Знаменатель агрегация", AGGREGATION_FUNCTIONS, key="denom_agg")
    denom_cond = st.text_input("Знаменатель условие (если maxIf/countIf)", key="denom_cond")
    denom_then = st.text_input("Знаменатель значение если условие верно", key="denom_then")

ratio_label = st.text_input("Название ratio-метрики", key="ratio_label")

# Добавляем индивидуальные WHERE фильтры для ratio-метрики
st.write("#### 🔍 Индивидуальные WHERE фильтры для ratio-метрики (опционально)")

if "temp_ratio_where_filters" not in st.session_state:
    st.session_state.temp_ratio_where_filters = []

col1, col2, col3, col4 = st.columns(4)
with col1:
    rwf_field = st.text_input("Поле", key="rwf_field")
with col2:
    rwf_op = st.selectbox("Оператор", ["=", "!=", "IN", ">=", "<=", ">", "<"], key="rwf_op")
with col3:
    rwf_value = st.text_input("Значение", key="rwf_value")
with col4:
    if st.button("➕ Добавить фильтр", key="add_ratio_filter"):
        if rwf_field and rwf_op and rwf_value:
            val = [v.strip() for v in rwf_value.split(",")] if rwf_op == "IN" else rwf_value.strip()
            st.session_state.temp_ratio_where_filters.append({
                "field": rwf_field,
                "operator": rwf_op,
                "value": val
            })
            st.rerun()

if st.session_state.temp_ratio_where_filters:
    st.write("**Текущие фильтры для ratio-метрики:**")
    for i, f in enumerate(st.session_state.temp_ratio_where_filters):
        col1, col2 = st.columns([5, 1])
        with col1:
            st.markdown(f"- `{f['field']} {f['operator']} {f['value']}`")
        with col2:
            if st.button("❌", key=f"delete_temp_rwf_{i}"):
                st.session_state.temp_ratio_where_filters.pop(i)
                st.rerun()

if st.button("➕ Добавить ratio"):
    num_expr = f"{num_agg}({num_then}, {num_cond})" if num_agg in ["maxIf", "countIf"] and num_cond else f"{num_agg}({num_metric})"
    denom_expr = f"{denom_agg}({denom_then}, {denom_cond})" if denom_agg in ["maxIf", "countIf"] and denom_cond else f"{denom_agg}({denom_metric})"
    label_final = ratio_label if ratio_label else f"{num_expr} / {denom_expr}"
    new_ratio_metric = {
        "name": label_final, 
        "type": "ratio", 
        "numerator": num_expr, 
        "denominator": denom_expr,
        "where_filters": st.session_state.temp_ratio_where_filters.copy()
    }
    if new_ratio_metric not in st.session_state.metrics:
        st.session_state.metrics.append(new_ratio_metric)
        st.session_state.temp_ratio_where_filters = []  # Очищаем временные фильтры
        st.success(f"✅ Ratio-метрика {label_final} добавлена")
        st.rerun()


if st.session_state.metrics:
    st.write("📋 Текущие метрики:")
    for i, m in enumerate(st.session_state.metrics):
        col1, col2 = st.columns([5, 1])
        with col1:
            desc = m['expression'] if m['type'] == 'basic' else f"{m['numerator']} / {m['denominator']}"
            filters_info = ""
            if m.get("where_filters"):
                filters_count = len(m["where_filters"])
                filters_info = f" *({filters_count} индивидуальных фильтров)*"
            st.markdown(f"- **{m['name']}** ({desc}){filters_info}")
        with col2:
            if st.button("❌", key=f"delete_metric_{i}"):
                st.session_state.metrics.pop(i)
                st.rerun()

# --- WHERE фильтры
st.write("## WHERE фильтры (глобальные для всех метрик)")
where_field = st.text_input("Поле", key='where_field')
where_op = st.selectbox("Оператор", ["=", "!=", "IN",">=","<=",">","<"], key='where_op')
where_value = st.text_input("Значение (добавь кавычки для строк, без кавычек для чисел)", key='where_value')

if st.button("➕ Добавить WHERE"):
    if where_field and where_op and where_value:
        val = [v.strip() for v in where_value.split(",")] if where_op == "IN" else where_value.strip()
        st.session_state.where_filters.append({
            "field": where_field,
            "operator": where_op,
            "value": val
        })
        st.success("✅ WHERE фильтр добавлен")
        st.rerun()

if st.session_state.where_filters:
    st.write("📋 WHERE фильтры:")
    for i, f in enumerate(st.session_state.where_filters):
        col1, col2 = st.columns([5, 1])
        with col1:
            st.markdown(f"- `{f['field']} {f['operator']} {f['value']}`")
        with col2:
            if st.button("❌", key=f"delete_where_{i}"):
                st.session_state.where_filters.pop(i)
                st.rerun()

# --- HAVING фильтры
st.write("## HAVING фильтры")
having_expr = st.text_input("HAVING выражение (например: sum(money_paid) > 100)", key='having_expr')

if st.button("➕ Добавить HAVING"):
    if having_expr:
        st.session_state.having_filters.append({"expression": having_expr.strip()})
        st.success("✅ HAVING фильтр добавлен")
        st.rerun()

if st.session_state.having_filters:
    st.write("📋 HAVING фильтры:")
    for i, h in enumerate(st.session_state.having_filters):
        col1, col2 = st.columns([5, 1])
        with col1:
            st.markdown(f"- `{h['expression']}`")
        with col2:
            if st.button("❌", key=f"delete_having_{i}"):
                st.session_state.having_filters.pop(i)
                st.rerun()

# --- Предпросмотр SQL
st.write("## 🧪 Предпросмотр SQL по текущим параметрам")
if st.button("👀 Сгенерировать SQL для текущего эксперимента"):
    preview_exp = {
        "experiment_name": exp_name,
        "control_group_id": control_id,
        "test_group_id": test_id,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "metrics": st.session_state.metrics,
        "filters": {
            "where": st.session_state.where_filters,
            "having": st.session_state.having_filters
        }
    }
    queries = generate_sql_queries_for_metrics(preview_exp, "ft_pa_prod.delivery_abtest_metrics_daily")
    for name, sql in queries:
        st.markdown(f"### {name}")
        st.code(sql, language="sql")

# --- Сохранение эксперимента
if st.button("💾 Сохранить эксперимент"):
    if not exp_name or not st.session_state.metrics:
        st.error("❌ Укажи название и хотя бы одну метрику.")
    else:
        new_exp = {
            "experiment_name": exp_name,
            "control_group_id": control_id,
            "test_group_id": test_id,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "metrics": st.session_state.metrics,
            "filters": {
                "where": st.session_state.where_filters,
                "having": st.session_state.having_filters
            }
        }

        config_data["experiments"] = [e for e in config_data["experiments"] if e["experiment_name"] != exp_name]
        config_data["experiments"].append(new_exp)

        with open(CONFIG_FILE, "w") as f:
            yaml.dump(config_data, f, sort_keys=False, allow_unicode=True)

        st.success(f"✅ Эксперимент '{exp_name}' добавлен!")
        st.session_state.where_filters = []
        st.session_state.having_filters = []
        st.session_state.metrics = []
        st.session_state.temp_metric_where_filters = []
        st.session_state.temp_ratio_where_filters = []
        st.session_state.editing_experiment = None

# --- Удаление эксперимента
st.write("## 🧹 Удаление эксперимента из YAML")
if existing_names:
    exp_to_delete = st.selectbox("Выбери эксперимент для удаления", existing_names)
    if st.button(f"❌ Удалить эксперимент '{exp_to_delete}'"):
        config_data["experiments"] = [e for e in config_data["experiments"] if e["experiment_name"] != exp_to_delete]
        with open(CONFIG_FILE, "w") as f:
            yaml.dump(config_data, f, sort_keys=False, allow_unicode=True)
        st.success(f"Эксперимент '{exp_to_delete}' удалён из конфига")
        st.rerun()

# --- Создание и сохранение пресетов
st.write("## 💾 Создание пресета метрики")
st.write("*Сохрани текущую настроенную метрику как пресет для будущего использования*")

if st.session_state.metrics:
    preset_metric_idx = st.selectbox("Выбери метрику для сохранения как пресет", 
                                   range(len(st.session_state.metrics)),
                                   format_func=lambda x: st.session_state.metrics[x]["name"])
    
    preset_name = st.text_input("Название пресета", key="preset_name")
    
    if st.button("💾 Сохранить как пресет"):
        if preset_name:
            selected_metric = st.session_state.metrics[preset_metric_idx].copy()
            selected_metric["name"] = preset_name  # Перезаписываем название
            save_new_preset(selected_metric)
            st.success(f"✅ Пресет '{preset_name}' сохранен!")
            # Перезагружаем пресеты
            metrics_presets = load_presets()
        else:
            st.error("❌ Укажи название пресета")

