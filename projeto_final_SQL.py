import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import numpy as np

# Configuração da Página
st.set_page_config(page_title="Analytics Olist - Tratamento Avançado", layout="wide")

# --- CONEXÃO ---
@st.cache_resource
def get_db_connection():
    DB_HOST = "ip-45-79-142-173.cloudezapp.io"
    DB_PORT = "3306"
    DB_USER = "alunosqlharve"
    DB_PASS = "Ed&ktw35j"
    DB_NAME = "modulosql"
    return create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# --- FUNÇÃO DE LIMPEZA AVANÇADA ---
def safe_date_parse(series):
    """Tenta converter datas de forma robusta, lidando com formatos mistos."""
    # Se já for data, retorna direto
    if pd.api.types.is_datetime64_any_dtype(series):
        return series
    
    # Converte para string e limpa
    s = series.astype(str).str.strip().replace('nan', pd.NA).replace('None', pd.NA)
    
    # Tenta formato ISO (YYYY-MM-DD) primeiro (Padrão SQL)
    try:
        return pd.to_datetime(s, format='%Y-%m-%d %H:%M:%S', errors='coerce')
    except:
        # Se falhar, tenta formato BR (DD/MM/YYYY) com dayfirst=True
        return pd.to_datetime(s, dayfirst=True, errors='coerce')

# --- CARREGAMENTO ---
@st.cache_data(ttl=600) # Cache curto para testes
def load_data_v3():
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            # Carrega tabelas
            orders = pd.read_sql("SELECT * FROM olist_orders_dataset", conn)
            items = pd.read_sql("SELECT * FROM olist_order_items_dataset", conn)
            products = pd.read_sql("SELECT * FROM olist_products_dataset", conn)
            customers = pd.read_sql("SELECT * FROM olist_customers_dataset", conn)
            sellers = pd.read_sql("SELECT * FROM olist_sellers_dataset", conn)
            payments = pd.read_sql("SELECT * FROM olist_order_payments_dataset", conn)
            reviews = pd.read_sql("SELECT * FROM olist_order_reviews_dataset", conn)

        # ---------------------------------------------------------
        # CAMADA 1: TRATAMENTO DE DATAS NA ORIGEM
        # ---------------------------------------------------------
        date_cols = ['order_purchase_timestamp', 'order_approved_at', 
                     'order_delivered_customer_date', 'order_estimated_delivery_date']
        
        for col in date_cols:
            if col in orders.columns:
                orders[col] = safe_date_parse(orders[col])

        # ---------------------------------------------------------
        # CAMADA 2: JOIN E CÁLCULO DE VALORES (EVITAR DUPLICIDADE)
        # ---------------------------------------------------------
        
        # Base: Pedidos + Clientes (1:1)
        df = pd.merge(orders, customers, on='customer_id', how='left')
        
        # + Itens (1:N) - Aqui o pedido pode "explodir" em linhas se tiver varios itens
        df = pd.merge(df, items, on='order_id', how='left')
        
        # Tratamento de Valores (Substitui vírgula e garante float)
        for col in ['price', 'freight_value']:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)
            
        # Valor Total da Linha (Item + Frete)
        df['total_payment'] = df['price'] + df['freight_value']

        # Enriquecimento
        df = pd.merge(df, products, on='product_id', how='left')
        df = pd.merge(df, sellers, on='seller_id', how='left')

        # ---------------------------------------------------------
        # CAMADA 3: PAGAMENTOS E REVIEWS (DEDUPLICAÇÃO ESTRITA)
        # ---------------------------------------------------------
        # Para evitar que um pedido de R$100 com 3 parcelas vire R$300
        
        # Pagamentos: Pegamos apenas os dados qualitativos (tipo, parcelas) do MAIOR pagamento
        if not payments.empty:
            payments['payment_value'] = pd.to_numeric(payments['payment_value'].astype(str).str.replace(',', '.'), errors='coerce')
            pay_dedup = payments.sort_values('payment_value', ascending=False).drop_duplicates('order_id')
            df = pd.merge(df, pay_dedup[['order_id', 'payment_type', 'payment_installments']], on='order_id', how='left')

        # Reviews: Pegamos apenas o primeiro review
        if not reviews.empty:
            if 'review_comment_message' in reviews.columns:
                reviews = reviews.rename(columns={'review_comment_message': 'review_comment'})
            rev_dedup = reviews.drop_duplicates('order_id')
            df = pd.merge(df, rev_dedup[['order_id', 'review_score', 'review_comment']], on='order_id', how='left')
            
            # Garante que review_score é número
            df['review_score'] = pd.to_numeric(df['review_score'], errors='coerce')

        return df

    except Exception as e:
        st.error(f"Erro crítico no processamento: {e}")
        st.stop()

# Executa carregamento
df = load_data_v3()

# ---------------------------------------------------------
# CAMADA 4: FILTRO DE DADOS SUJOS (OUTLIERS)
# ---------------------------------------------------------

# Filtro 1: Apenas entregues (para cálculo de prazo)
df_delivered = df[df['order_status'] == 'delivered'].copy()

# Cálculo de Dias
df_delivered['days_to_delivery'] = (df_delivered['order_delivered_customer_date'] - df_delivered['order_approved_at']).dt.days

# Filtro 2: Remoção de Datas Absurdas (Erro de sistema ou logística extrema)
# Removemos prazos negativos (erro) ou maiores que 180 dias (provável extravio/erro)
mask_valid_dates = (df_delivered['days_to_delivery'] >= 0) & (df_delivered['days_to_delivery'] <= 180)
df_clean_logistics = df_delivered[mask_valid_dates]

# Para Vendas, usamos o df completo (pois venda cancelada também tem valor tentado)
# Mas vamos usar df_delivered para "Vendas Efetivadas" se preferir. Vamos usar df geral.

# =========================================================
# DASHBOARD
# =========================================================

st.title("📊 Dashboard Executivo (Tratamento Avançado)")

if st.sidebar.checkbox("🕵️ Mostrar Diagnóstico de Dados"):
    st.header("Diagnóstico")
    st.write("Amostra dos dados processados:")
    st.dataframe(df.head())
    st.write(f"Total Linhas: {len(df)}")
    st.write(f"Pedidos Únicos: {df['order_id'].nunique()}")
    
    n_sujos = len(df_delivered) - len(df_clean_logistics)
    st.warning(f"Foram removidos {n_sujos} registros com datas inconsistentes (negativas ou > 180 dias) para o cálculo de média.")

# Métricas Globais (Topo)
c1, c2, c3, c4 = st.columns(4)
# Métrica 1: Faturamento (Soma correta)
faturamento = df['total_payment'].sum()
c1.metric("Faturamento Total", f"R$ {faturamento:,.2f}")

# Métrica 2: Pedidos (Contagem Única)
pedidos = df['order_id'].nunique()
c2.metric("Total Pedidos", f"{pedidos:,}")

# Métrica 3: Prazo Médio (Usando base limpa)
prazo_medio = df_clean_logistics['days_to_delivery'].mean()
c3.metric("Prazo Médio Entrega", f"{prazo_medio:.1f} dias")

# Métrica 4: Nota Média
nota_media = df['review_score'].mean()
c4.metric("Nota Média", f"{nota_media:.2f}/5")

st.markdown("---")

# ABAS
abas = st.tabs(["📦 Logística", "💰 Vendas", "⭐ Satisfação", "🏷️ Produtos", "🗺️ Geografia", "🔄 Recompra"])

with abas[0]: # Logística
    st.subheader("Análise de Entrega (Base Limpa)")
    col1, col2 = st.columns(2)
    
    # Histograma
    fig_hist = px.histogram(df_clean_logistics, x='days_to_delivery', nbins=50, title="Distribuição Real do Prazo")
    col1.plotly_chart(fig_hist, use_container_width=True)
    
    # Status do Prazo
    df_clean_logistics['status_prazo'] = df_clean_logistics.apply(
        lambda x: 'No Prazo' if x['order_delivered_customer_date'] <= x['order_estimated_delivery_date'] else 'Atrasado', axis=1
    )
    fig_pie = px.pie(df_clean_logistics, names='status_prazo', title="Entregas no Prazo vs Atrasadas", color_discrete_sequence=['green', 'red'])
    col2.plotly_chart(fig_pie, use_container_width=True)

with abas[1]: # Vendas
    st.subheader("Evolução de Vendas")
    # Agrupamento correto por mês
    df['mes_ano'] = df['order_purchase_timestamp'].dt.to_period('M').astype(str)
    # Importante: Somamos valor e contamos IDs únicos de pedido
    vendas_mes = df.groupby('mes_ano').agg({
        'total_payment': 'sum',
        'order_id': 'nunique'
    }).reset_index()
    
    # Identifica pico
    if not vendas_mes.empty:
        pico = vendas_mes.loc[vendas_mes['total_payment'].idxmax()]
        st.info(f"🏆 Melhor Mês: **{pico['mes_ano']}** com R$ {pico['total_payment']:,.2f}")
    
    fig_line = px.line(vendas_mes, x='mes_ano', y='total_payment', markers=True, title="Faturamento Mensal")
    st.plotly_chart(fig_line, use_container_width=True)

with abas[2]: # Satisfação
    st.subheader("Avaliações")
    c1, c2 = st.columns([1, 2])
    # Comentários
    comentarios = df['review_comment'].notna().mean() * 100
    c1.metric("% Com Clientes que Comentam", f"{comentarios:.1f}%")
    # Gráfico Notas
    fig_bar = px.bar(df['review_score'].value_counts().reset_index(), x='review_score', y='count', title="Distribuição das Notas")
    c2.plotly_chart(fig_bar, use_container_width=True)

with abas[3]: # Produtos
    st.subheader("Top Categorias")
    # Limpa categorias nulas
    df_cat = df.dropna(subset=['product_category_name'])
    df_cat['Categoria'] = df_cat['product_category_name'].str.replace('_', ' ').str.title()
    
    top_cat = df_cat.groupby('Categoria')['total_payment'].sum().nlargest(10).reset_index()
    fig_cat = px.bar(top_cat, x='total_payment', y='Categoria', orientation='h', title="Top 10 Categorias por Receita")
    fig_cat.update_layout(yaxis={'categoryorder':'total ascending'})
    st.plotly_chart(fig_cat, use_container_width=True)

    # Relação Preço x Vendas
    st.markdown("#### Preço vs Volume")
    scatter_data = df_cat.groupby('Categoria').agg({
        'price': 'mean',
        'order_id': 'count'
    }).reset_index()
    fig_scat = px.scatter(scatter_data, x='price', y='order_id', hover_name='Categoria', 
                          title="Preço Médio vs Quantidade Vendida", labels={'price': 'Preço Médio', 'order_id': 'Qtd Vendas'})
    st.plotly_chart(fig_scat, use_container_width=True)

with abas[4]: # Geografia
    st.subheader("Geografia")
    col1, col2 = st.columns(2)
    # Filtra Nulos
    df_geo = df.dropna(subset=['customer_state', 'seller_state'])
    
    top_cli = df_geo['customer_state'].value_counts().head(10)
    col1.plotly_chart(px.bar(top_cli, title="Top Estados (Clientes)"), use_container_width=True)
    
    top_sel = df_geo['seller_state'].value_counts().head(10)
    col2.plotly_chart(px.bar(top_sel, title="Top Estados (Vendedores)", color_discrete_sequence=['orange']), use_container_width=True)

with abas[5]: # Recompra
    st.subheader("Fidelidade (Recompra)")
    if 'customer_zip_code_prefix' in df.columns and 'customer_city' in df.columns:
        # Proxy de ID
        df['proxy_id'] = df['customer_zip_code_prefix'].astype(str) + df['customer_city']
        contagem = df.groupby('proxy_id')['order_id'].nunique()
        recorrentes = contagem[contagem > 1].index
        
        df_rec = df[df['proxy_id'].isin(recorrentes)]
        
        if not df_rec.empty:
            c1, c2 = st.columns(2)
            c1.metric("Clientes Recorrentes", f"{len(recorrentes):,}")
            c2.metric("Ticket Médio (Recorrentes)", f"R$ {df_rec['total_payment'].mean():.2f}")
            
            # Perfil
            st.write("Preferência de Pagamento na Recompra:")
            fig_pag = px.pie(df_rec.drop_duplicates('order_id'), names='payment_type', title="Meio de Pagamento")
            st.plotly_chart(fig_pag, use_container_width=True)
        else:
            st.warning("Poucos dados para análise de recompra.")