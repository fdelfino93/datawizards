import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import os

# Configuração da Página
st.set_page_config(page_title="Analytics Olist - Dados Reais", layout="wide")

# --- FUNÇÃO DE CARREGAMENTO E FUSÃO DE DADOS (SQL) ---
@st.cache_data
def load_data():
    # Credenciais MySQL fornecidas
    DB_HOST = "ip-45-79-142-173.cloudezapp.io"
    DB_PORT = "3306"
    DB_USER = "alunosqlharve"
    DB_PASS = "Ed&ktw35j"
    DB_NAME = "modulosql"

    try:
        # Criando a engine de conexão com MySQL
        engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        
        with engine.connect() as conn:
            # Carregamento das tabelas mantendo os nomes originais
            df_orders = pd.read_sql("SELECT * FROM olist_orders_dataset", conn)
            df_items = pd.read_sql("SELECT * FROM olist_order_items_dataset", conn)
            df_products = pd.read_sql("SELECT * FROM olist_products_dataset", conn)
            df_customers = pd.read_sql("SELECT * FROM olist_customers_dataset", conn)
            df_sellers = pd.read_sql("SELECT * FROM olist_sellers_dataset", conn)
            df_payments = pd.read_sql("SELECT * FROM olist_order_payments_dataset", conn)
            df_reviews = pd.read_sql("SELECT * FROM olist_order_reviews_dataset", conn)

        # --- FUSÃO DAS TABELAS (Estratégia LEFT JOIN) ---
        
        # Base: Pedidos
        df = df_orders.copy()
        
        # + Itens
        df = pd.merge(df, df_items, on='order_id', how='left')
        
        # + Produtos
        df = pd.merge(df, df_products, on='product_id', how='left')
        
        # + Clientes
        df = pd.merge(df, df_customers, on='customer_id', how='left')
        
        # + Vendedores
        df = pd.merge(df, df_sellers, on='seller_id', how='left')
        
        # + Pagamentos (CORREÇÃO CRÍTICA DE DUPLICIDADE)
        if not df_payments.empty:
            df_payments['payment_value'] = pd.to_numeric(df_payments['payment_value'].astype(str).str.replace(',', '.'), errors='coerce')
            # Ordena por valor e mantém apenas o maior pagamento por pedido
            df_payments_unique = df_payments.sort_values('payment_value', ascending=False).drop_duplicates(subset=['order_id'], keep='first')
            df = pd.merge(df, df_payments_unique, on='order_id', how='left')
        
        # + Reviews (CORREÇÃO DE DUPLICIDADE E CORREÇÃO DO ERRO DE 99%)
        if 'review_comment_message' in df_reviews.columns:
            # Substitui strings vazias por nulo para o cálculo de porcentagem correto
            df_reviews['review_comment_message'] = df_reviews['review_comment_message'].replace('', pd.NA)
            df_reviews = df_reviews.rename(columns={'review_comment_message': 'review_comment'})
        
        # Remove duplicatas de reviews (mantém a primeira encontrada)
        df_reviews_unique = df_reviews.drop_duplicates(subset=['order_id'], keep='first')
        df = pd.merge(df, df_reviews_unique, on='order_id', how='left')

        # --- LIMPEZA E TIPAGEM ---
        
        # Converter colunas numéricas
        numeric_cols = ['price', 'freight_value', 'product_weight_g', 'product_photos_qty', 'review_score', 'payment_installments']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)

        # Cálculo do Valor Total (Item + Frete)
        df['total_payment'] = df['price'] + df['freight_value']

        # Renomear colunas para Português
        mapa_colunas = {
            'order_status': 'Status do Pedido',
            'order_purchase_timestamp': 'Data da Compra',
            'order_approved_at': 'Data Aprovação',
            'order_delivered_customer_date': 'Data Entrega Real',
            'order_estimated_delivery_date': 'Data Entrega Prevista',
            'customer_state': 'Estado do Cliente',
            'seller_state': 'Estado do Vendedor',
            'total_payment': 'Valor Total',
            'payment_type': 'Tipo de Pagamento',
            'payment_installments': 'Parcelas',
            'review_score': 'Nota de Avaliação',
            'review_comment': 'Comentário',
            'product_category_name': 'Categoria do Produto',
            'product_photos_qty': 'Qtd Fotos',
            'product_weight_g': 'Peso (g)',
            'price': 'Preço Unitário',
            'freight_value': 'Valor do Frete',
            'product_length_cm': 'Comprimento (cm)',
            'product_height_cm': 'Altura (cm)',
            'product_width_cm': 'Largura (cm)',
            'customer_city': 'Cidade do Cliente',
            'customer_zip_code_prefix': 'CEP Prefixo'
        }
        df = df.rename(columns=mapa_colunas)
        
        # Conversão de Datas
        cols_data = ['Data da Compra', 'Data Aprovação', 'Data Entrega Real', 'Data Entrega Prevista']
        for col in cols_data:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        return df

    except Exception as e:
        st.error(f"Erro ao processar dados via MySQL: {e}")
        st.stop()

# Carrega os dados
df = load_data()

# --- VERIFICAÇÃO DE SEGURANÇA ---
if df.empty:
    st.error("Erro Crítico: A tabela final ficou vazia.")
    st.stop()

st.title("🚀 Dashboard Executivo (Dados Validados)")
st.caption(f"Base carregada via MySQL: {len(df):,} registros processados.")

# --- ENGENHARIA DE NOVAS COLUNAS ---
# Q1: Dias para Entrega
if {'Data Entrega Real', 'Data Aprovação'}.issubset(df.columns):
    df['Dias para Entrega'] = (df['Data Entrega Real'] - df['Data Aprovação']).dt.days

# Q4: Status do Prazo
if {'Data Entrega Real', 'Data Entrega Prevista'}.issubset(df.columns):
    df['Entregue no Prazo'] = df['Data Entrega Real'] <= df['Data Entrega Prevista']
    df['Status do Prazo'] = df['Entregue no Prazo'].map({True: 'No Prazo/Adiantado', False: 'Atrasado'})

# Q6: Volume
if {'Comprimento (cm)', 'Altura (cm)', 'Largura (cm)'}.issubset(df.columns):
    df['Volume (cm3)'] = df['Comprimento (cm)'] * df['Altura (cm)'] * df['Largura (cm)']

# Q8: Tipo de Frete
if {'Estado do Cliente', 'Estado do Vendedor'}.issubset(df.columns):
    df['Estado do Cliente'] = df['Estado do Cliente'].fillna('Desc')
    df['Estado do Vendedor'] = df['Estado do Vendedor'].fillna('Desc')
    df['Tipo de Frete'] = df.apply(lambda x: 'Local' if x['Estado do Cliente'] == x['Estado do Vendedor'] and x['Estado do Cliente'] != 'Desc' else 'Interestadual', axis=1)

# Q9: Recompra (Proxy)
if {'CEP Prefixo', 'Cidade do Cliente'}.issubset(df.columns):
    df['ID Cliente (Proxy)'] = df['CEP Prefixo'].astype(str) + "_" + df['Cidade do Cliente']
    contagem = df['ID Cliente (Proxy)'].value_counts()
    recorrentes = contagem[contagem > 1].index
    df['Cliente Recorrente'] = df['ID Cliente (Proxy)'].isin(recorrentes)
    df['Tipo de Cliente'] = df['Cliente Recorrente'].map({True: 'Recorrente', False: 'Novo'})

# Tratamento de Categoria
if 'Categoria do Produto' in df.columns:
    df['Categoria do Produto'] = df['Categoria do Produto'].astype(str).str.replace('_', ' ').str.title()

# --- VISUALIZAÇÃO ---
abas = st.tabs([
    "📦 Logística", 
    "💰 Vendas", 
    "⭐ Satisfação", 
    "🏷️ Produtos", 
    "🗺️ Geografia", 
    "🔄 Recompra"
])

# ABA 1: Logística
with abas[0]:
    st.subheader("Performance Logística")
    st.caption("Q1: Tempo desde a aprovação do pedido até a entrega ao cliente. | Q6: Impacto do peso e volume no frete. | Q8: Atrasos por tipo de frete.")
    col1, col2 = st.columns(2)

    if 'Dias para Entrega' in df.columns:
        media_dias = df['Dias para Entrega'].mean()
        mediana_dias = df['Dias para Entrega'].median()
        val_media = f"{media_dias:.1f}" if pd.notna(media_dias) else "N/A"
        val_mediana = f"{mediana_dias:.1f}" if pd.notna(mediana_dias) else "N/A"
        m1, m2 = col1.columns(2)
        m1.metric("Tempo Médio de Entrega", f"{val_media} dias", help="Média aritmética dos dias entre aprovação e entrega")
        m2.metric("Tempo Mediano de Entrega", f"{val_mediana} dias", help="Valor central: 50% das entregas levam menos que isso")
        fig = px.histogram(df, x='Dias para Entrega', nbins=30, title="Curva de Entrega (Dias)")
        col1.plotly_chart(fig, use_container_width=True)

    if 'Status do Prazo' in df.columns:
        valid_prazos = df['Status do Prazo'].dropna()
        if not valid_prazos.empty:
            pct_prazo = (valid_prazos == 'No Prazo/Adiantado').mean() * 100
            col2.metric("% No Prazo", f"{pct_prazo:.1f}%")
            fig_pz = px.pie(df, names='Status do Prazo', title="Aderência ao Prazo", hole=0.4, color_discrete_sequence=['green', 'red'])
            col2.plotly_chart(fig_pz, use_container_width=True)
    
    c3, c4 = st.columns(2)
    if 'Peso (g)' in df.columns and 'Valor do Frete' in df.columns:
        df_peso = df[(df['Peso (g)'] > 0) & (df['Valor do Frete'] > 0)]
        if not df_peso.empty:
            fig_peso = px.scatter(df_peso.sample(min(2000, len(df_peso))), x='Peso (g)', y='Valor do Frete', title="Peso x Frete (Amostra)", opacity=0.5)
            c3.plotly_chart(fig_peso, use_container_width=True)
            c3.caption("Cada ponto representa um produto. Quanto mais pesado, maior tende a ser o frete.")

    if 'Volume (cm3)' in df.columns and 'Valor do Frete' in df.columns:
        df_vol = df[(df['Volume (cm3)'] > 0) & (df['Valor do Frete'] > 0)]
        if not df_vol.empty:
            fig_vol = px.scatter(df_vol.sample(min(2000, len(df_vol))), x='Volume (cm3)', y='Valor do Frete', title="Volume x Frete (Amostra)", opacity=0.5)
            c4.plotly_chart(fig_vol, use_container_width=True)
            c4.caption("Volume = Comprimento x Altura x Largura do produto. Analisa se o tamanho físico impacta no frete.")

    c5, c6 = st.columns(2)
    if 'Tipo de Frete' in df.columns and 'Status do Prazo' in df.columns:
        atrasos = df[df['Status do Prazo'] == 'Atrasado']
        if not atrasos.empty:
            fig_atr = px.histogram(atrasos, x='Tipo de Frete', title="Onde ocorrem os atrasos?")
            c5.plotly_chart(fig_atr, use_container_width=True)
            c5.caption("Local = vendedor e comprador no mesmo estado. Interestadual = estados diferentes.")

# ABA 2: Vendas
with abas[1]:
    st.subheader("Análise Financeira")
    st.caption("Q2: Mês com maior volume de pedidos e mês com maior faturamento (Preço + Frete).")
    if 'Data da Compra' in df.columns:
        df_vendas = df.dropna(subset=['Data da Compra']).copy()
        if not df_vendas.empty:
            df_vendas['Mês/Ano'] = df_vendas['Data da Compra'].dt.to_period('M').astype(str)
            
            vendas_mes = df_vendas.groupby('Mês/Ano').agg(
                Qtd_Pedidos=('Status do Pedido', 'count'),
                Faturamento=('Valor Total', 'sum')
            ).reset_index()
            
            if not vendas_mes.empty:
                idx_ped = vendas_mes['Qtd_Pedidos'].idxmax()
                idx_fat = vendas_mes['Faturamento'].idxmax()
                pico_ped = vendas_mes.loc[idx_ped]
                pico_fat = vendas_mes.loc[idx_fat]
                
                c1, c2 = st.columns(2)
                c1.info(f"📅 Mais Pedidos: **{pico_ped['Mês/Ano']}** ({pico_ped['Qtd_Pedidos']})")
                c2.success(f"💰 Maior Faturamento: **{pico_fat['Mês/Ano']}** (R$ {pico_fat['Faturamento']:,.2f})")
                
                st.plotly_chart(px.line(vendas_mes, x='Mês/Ano', y='Faturamento', markers=True, title="Faturamento Mensal"), use_container_width=True)

# ABA 3: Satisfação (CORREÇÃO APLICADA AQUI)
with abas[2]:
    st.subheader("NPS & Comentários")
    st.caption("Q3: Avaliação da satisfação dos clientes (notas de 1 a 5 e % que deixaram comentários). | Q4: Relação entre prazo de entrega e nota dada pelo cliente.")
    if 'Nota de Avaliação' in df.columns:
        # Deduplicação por pedido para métricas de satisfação (evita inflar por itens do mesmo pedido)
        df_satisfacao = df.drop_duplicates(subset=['order_id'])
        
        media = df_satisfacao['Nota de Avaliação'].mean()
        val_media = f"{media:.2f}" if pd.notna(media) else "0.00"
        
        c1, c2 = st.columns([1,2])
        c1.metric("Nota Média", f"{val_media}/5")
        
        if 'Comentário' in df_satisfacao.columns:
            # Cálculo sobre pedidos únicos: Comentários Válidos / Total de Pedidos
            total_pedidos = len(df_satisfacao)
            comentarios_reais = df_satisfacao['Comentário'].notna().sum()
            pct_coments = (comentarios_reais / total_pedidos) * 100
            c1.metric("% Comentaram", f"{pct_coments:.1f}%")
        
        df_notas = df_satisfacao['Nota de Avaliação'].dropna()
        if not df_notas.empty:
            fig_notas = px.bar(df_notas.value_counts().reset_index(), x='Nota de Avaliação', y='count', title="Distribuição de Notas (Pedidos Únicos)")
            c2.plotly_chart(fig_notas, use_container_width=True)

        # Q4: Cruzamento Prazo x Satisfação
        if 'Status do Prazo' in df_satisfacao.columns:
            st.markdown("#### Q4: Satisfação x Cumprimento do Prazo")
            st.caption("Compara a nota de avaliação dos clientes que receberam no prazo (ou adiantado) vs. os que receberam atrasado.")
            df_prazo_sat = df_satisfacao.dropna(subset=['Status do Prazo', 'Nota de Avaliação'])
            if not df_prazo_sat.empty:
                q4_c1, q4_c2 = st.columns(2)
                media_por_prazo = df_prazo_sat.groupby('Status do Prazo')['Nota de Avaliação'].mean().reset_index()
                fig_prazo_nota = px.bar(media_por_prazo, x='Status do Prazo', y='Nota de Avaliação',
                    color='Status do Prazo', color_discrete_map={'No Prazo/Adiantado': 'green', 'Atrasado': 'red'},
                    title="Nota Média por Status do Prazo", text_auto='.2f')
                q4_c1.plotly_chart(fig_prazo_nota, use_container_width=True)

                dist_prazo_nota = df_prazo_sat.groupby(['Status do Prazo', 'Nota de Avaliação']).size().reset_index(name='Quantidade')
                fig_dist = px.bar(dist_prazo_nota, x='Nota de Avaliação', y='Quantidade', color='Status do Prazo',
                    barmode='group', color_discrete_map={'No Prazo/Adiantado': 'green', 'Atrasado': 'red'},
                    title="Distribuição de Notas: No Prazo vs Atrasado")
                q4_c2.plotly_chart(fig_dist, use_container_width=True)

# ABA 4: Produtos
with abas[3]:
    st.subheader("Análise de Produtos")
    st.caption("Q5: Categorias mais e menos vendidas, relação entre preço e volume de vendas, e impacto da quantidade de fotos do anúncio nas vendas.")
    if 'Categoria do Produto' in df.columns:
        df_cat = df[df['Categoria do Produto'] != 'Nan'] 
        c1, c2 = st.columns(2)
        top_cats = df_cat['Categoria do Produto'].value_counts().head(10).reset_index()
        if not top_cats.empty:
            c1.plotly_chart(px.bar(top_cats, x='count', y='Categoria do Produto', orientation='h', title="Mais Vendidos"), use_container_width=True)
        bot_cats = df_cat['Categoria do Produto'].value_counts().tail(10).reset_index()
        if not bot_cats.empty:
            c2.plotly_chart(px.bar(bot_cats, x='count', y='Categoria do Produto', orientation='h', title="Menos Vendidos", color_discrete_sequence=['red']), use_container_width=True)

        if 'Preço Unitário' in df.columns:
            st.markdown("#### Preço x Volume de Vendas")
            st.caption("Cada ponto representa uma categoria. Eixo X = preço médio dos produtos da categoria. Eixo Y = quantidade de itens vendidos.")
            cat_perf = df_cat.groupby('Categoria do Produto').agg(
                Preco_Medio=('Preço Unitário', 'mean'),
                Vendas=('Status do Pedido', 'count')
            ).reset_index()
            if not cat_perf.empty:
                st.plotly_chart(px.scatter(cat_perf, x='Preco_Medio', y='Vendas', hover_name='Categoria do Produto'), use_container_width=True)

        # Q5: Fotos x Vendas
        if 'Qtd Fotos' in df.columns:
            st.markdown("#### Quantidade de Fotos do Anúncio x Vendas")
            st.caption("Qtd de Fotos = número de imagens que o vendedor cadastrou no anúncio do produto no marketplace. Analisa se anúncios com mais fotos vendem mais.")
            df_fotos = df_cat[df_cat['Qtd Fotos'] > 0].copy()
            if not df_fotos.empty:
                fotos_perf = df_fotos.groupby('Qtd Fotos').agg(
                    Vendas=('Status do Pedido', 'count'),
                    Preco_Medio=('Preço Unitário', 'mean')
                ).reset_index()
                f1, f2 = st.columns(2)
                fig_fotos = px.bar(fotos_perf, x='Qtd Fotos', y='Vendas', title="Volume de Vendas por Qtd de Fotos")
                f1.plotly_chart(fig_fotos, use_container_width=True)
                fig_fotos_preco = px.bar(fotos_perf, x='Qtd Fotos', y='Preco_Medio', title="Preço Médio por Qtd de Fotos", color_discrete_sequence=['orange'])
                f2.plotly_chart(fig_fotos_preco, use_container_width=True)

# ABA 5: Geografia
with abas[4]:
    st.subheader("Geografia")
    st.caption("Q7: Estados (UF) com maior concentração de compradores e de vendedores no marketplace.")
    c1, c2 = st.columns(2)
    if 'Estado do Cliente' in df.columns:
        c1.plotly_chart(px.bar(df['Estado do Cliente'].value_counts().head(10), title="Top Compradores (UF)"), use_container_width=True)
    if 'Estado do Vendedor' in df.columns:
        c2.plotly_chart(px.bar(df['Estado do Vendedor'].value_counts().head(10), title="Top Vendedores (UF)", color_discrete_sequence=['orange']), use_container_width=True)

# ABA 6: Recompra
with abas[5]:
    st.subheader("Perfil de Recompra")
    st.caption("Q9: Padrão dos clientes que compraram mais de uma vez. Identificados por combinação de CEP + Cidade (proxy, pois a Olist anonimiza os clientes).")
    if 'Tipo de Cliente' in df.columns:
        df_rec = df[df['Tipo de Cliente'] == 'Recorrente']
        if not df_rec.empty:
            c1, c2, c3 = st.columns(3)
            c1.metric("Clientes Recorrentes", df_rec['ID Cliente (Proxy)'].nunique())
            c2.metric("Ticket Médio", f"R$ {df_rec['Valor Total'].mean():.2f}")
            c3.metric("Nota Média", f"{df_rec['Nota de Avaliação'].mean():.2f}")
            
            # Métricas adicionais
            c4, c5, c6 = st.columns(3)
            if 'Parcelas' in df_rec.columns:
                c4.metric("Média de Parcelas", f"{df_rec['Parcelas'].mean():.1f}")
            if 'Status do Prazo' in df_rec.columns:
                prazo_rec = df_rec['Status do Prazo'].dropna()
                if not prazo_rec.empty:
                    pct_prazo_rec = (prazo_rec == 'No Prazo/Adiantado').mean() * 100
                    c5.metric("% Entrega no Prazo", f"{pct_prazo_rec:.1f}%")
            if 'Estado do Cliente' in df_rec.columns:
                top_estado = df_rec['Estado do Cliente'].value_counts().index[0]
                c6.metric("Estado com Mais Recompras", top_estado)

            r1, r2 = st.columns(2)
            if 'Tipo de Pagamento' in df_rec.columns:
                r1.plotly_chart(px.pie(df_rec, names='Tipo de Pagamento', title="Pagamento Preferido na Recompra"), use_container_width=True)
            if 'Categoria do Produto' in df_rec.columns:
                top_rec = df_rec[df_rec['Categoria do Produto'] != 'Nan']['Categoria do Produto'].value_counts().head(5).reset_index()
                r2.plotly_chart(px.bar(top_rec, x='count', y='Categoria do Produto', orientation='h', title="Top Categorias na Recompra"), use_container_width=True)

            r3, r4 = st.columns(2)
            if 'Estado do Cliente' in df_rec.columns:
                top_estados_rec = df_rec['Estado do Cliente'].value_counts().head(10).reset_index()
                fig_loc = px.bar(top_estados_rec, x='count', y='Estado do Cliente', orientation='h', title="Top Estados - Clientes Recorrentes")
                r3.plotly_chart(fig_loc, use_container_width=True)
            if 'Parcelas' in df_rec.columns:
                parcelas_dist = df_rec['Parcelas'].value_counts().sort_index().reset_index()
                parcelas_dist.columns = ['Parcelas', 'Quantidade']
                fig_parc = px.bar(parcelas_dist, x='Parcelas', y='Quantidade', title="Distribuição de Parcelas na Recompra")
                r4.plotly_chart(fig_parc, use_container_width=True)