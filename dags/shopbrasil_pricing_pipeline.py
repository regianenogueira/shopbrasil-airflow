import requests
import pendulum
from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import timedelta
from operators.schema_validator import ValidarProdutosOperator

# ==========================================
# 1. CALLBACKS DE CICLO DE VIDA E ALERTAS
# ==========================================
def on_failure_callback(context):
    task_instance = context.get('task_instance')
    print(f"[ALERTA CRÍTICO] A task {task_instance.task_id} falhou após todas as tentativas. Avisar time!")

def on_retry_callback(context):
    task_instance = context.get('task_instance')
    print(f"[RETRY] API instável. Retentando a task {task_instance.task_id}...")

def on_success_callback(context):
    print("[SUCESSO] Coleta da FakeStore API concluída perfeitamente!")

# ==========================================
# 2. DEFINIÇÃO DA DAG
# ==========================================
default_args = {
    'owner': 'tech_lead_dados',
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'retry_exponential_backoff': True,
    'on_failure_callback': on_failure_callback
}

@dag(
    dag_id='shopbrasil_pricing_daily',
    default_args=default_args,
    schedule='0 6 * * *',
    start_date=pendulum.timezone('America/Sao_Paulo').convert(pendulum.now() - timedelta(days=2)),
    catchup=False,
    tags=['shopbrasil', 'pricing']
)
def shopbrasil_pricing_pipeline():

    # ==========================================
    # 3. INGESTÃO DOS DADOS
    # ==========================================
    @task_group(group_id='ingestao_dados')
    def ingestao():

        # ==========================================
        # EXTRAÇÃO: BUSCAR PRODUTOS
        # Busca os produtos na FakeStore API e retorna o JSON.
        # Se a API der erro o Airflow tenta de novo (3 retries).
        # ==========================================
        @task(
            on_success_callback=on_success_callback,
            on_retry_callback=on_retry_callback
        )
        def buscar_produtos():
            url = "https://fakestoreapi.com/products"
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                print(f"Erro ao conectar na API: {e}")
                raise 

        # ==========================================
        # EXTRAIR CATEGORIAS
        # Pega as categorias sem repetir, pra gerar
        # uma task de métricas pra cada uma.
        # ==========================================
        @task
        def extrair_categorias(produtos):
            categorias = {produto['category'] for produto in produtos}
            return list(categorias)

        produtos_brutos = buscar_produtos()
        # ==========================================
        # VALIDAÇÃO DOS PRODUTOS
        # Operator criado em plugins/operators/schema_validator.py.
        # Confere id, title, price, category e se o preço é negativo.
        # ==========================================
        produtos = ValidarProdutosOperator(
            task_id='validar_produtos', produtos=produtos_brutos
        ).output
        categorias = extrair_categorias(produtos)

        return produtos, categorias

    # ==========================================
    # 4. ANÁLISE DAS MÉTRICAS
    # ==========================================
    @task_group(group_id='analise_metricas')
    def analise(produtos, categorias):

        # ==========================================
        # TRATAMENTO: MÉTRICAS POR CATEGORIA
        # Filtra os produtos da categoria e calcula quantidade,
        # preço médio, mínimo e máximo. Roda uma vez por categoria
        # (expand) e usa o pool ecommerce_pool.
        # ==========================================
        @task(pool='ecommerce_pool')
        def calcular_metricas_categoria(categoria, todos_produtos):
            produtos_cat = [p for p in todos_produtos if p['category'] == categoria]
            precos = [p['price'] for p in produtos_cat]
            return {
                'categoria': categoria,
                'qtd_produtos': len(precos),
                'preco_medio': round(sum(precos) / len(precos), 2),
                'preco_minimo': min(precos),
                'preco_maximo': max(precos)
            }

        # ==========================================
        # CONSOLIDAR RESULTADOS
        # Junta o resultado de todas as categorias numa lista só.
        # ==========================================
        @task
        def consolidar_resultados(metricas_mapeadas):
            return list(metricas_mapeadas)

        metricas = calcular_metricas_categoria.partial(todos_produtos=produtos).expand(categoria=categorias)
        return consolidar_resultados(metricas)

    # ==========================================
    # 5. CARGA NO BANCO
    # ==========================================
    @task_group(group_id='carga_banco')
    def carga(metricas_consolidadas, data_execucao=None):

        # ==========================================
        # CARGA: GRAVAR NO POSTGRES
        # Cria as tabelas se não existirem e grava as métricas.
        # No pricing_snapshot apaga o dia antes de inserir (pra não
        # duplicar se rodar de novo), no pricing_history só insere.
        # ==========================================
        @task
        def gravar_postgres(metricas, ds=None):
            hook = PostgresHook(postgres_conn_id='postgres_shopbrasil')
            
            setup_queries = [
                """
                CREATE TABLE IF NOT EXISTS pricing_snapshot (
                    run_date DATE,
                    categoria VARCHAR(100),
                    qtd_produtos INT,
                    preco_medio NUMERIC(10,2),
                    preco_minimo NUMERIC(10,2),
                    preco_maximo NUMERIC(10,2)
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS pricing_history (
                    run_date DATE,
                    categoria VARCHAR(100),
                    qtd_produtos INT,
                    preco_medio NUMERIC(10,2),
                    preco_minimo NUMERIC(10,2),
                    preco_maximo NUMERIC(10,2)
                );
                """
            ]
            for query in setup_queries:
                hook.run(query)

            hook.run(f"DELETE FROM pricing_snapshot WHERE run_date = '{ds}'")

            linhas = []
            for m in metricas:
                linhas.append((
                    ds, m['categoria'], m['qtd_produtos'], 
                    m['preco_medio'], m['preco_minimo'], m['preco_maximo']
                ))

            hook.insert_rows(
                table='pricing_snapshot',
                rows=linhas,
                target_fields=['run_date', 'categoria', 'qtd_produtos', 'preco_medio', 'preco_minimo', 'preco_maximo']
            )

            hook.insert_rows(
                table='pricing_history',
                rows=linhas,
                target_fields=['run_date', 'categoria', 'qtd_produtos', 'preco_medio', 'preco_minimo', 'preco_maximo']
            )
            
            print(f"[{len(linhas)}] categorias processadas e salvas com sucesso para a data {ds}.")

        gravar_postgres(metricas_consolidadas)

    produtos_brutos, lista_categorias = ingestao()
    metricas_finais = analise(produtos_brutos, lista_categorias)
    carga(metricas_finais)

dag_instance = shopbrasil_pricing_pipeline()