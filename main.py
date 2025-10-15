#Execução de consulta máxima para todos tags escolhidos (Em ambiente corporativo)
import pandas as pd
import datetime
from sqlalchemy.orm import sessionmaker
from models.sensores_db import LeituraSensor, Sensores, engine, Base
from osisoft.pidevclub.piwebapi.pi_web_api_client import PIWebApiClient
#Melhora a pesquisa para encontrar sensores específicos e transforma em DataFrame

#Parâmetros de execução
#Execução para

def get_tags(client_, tag_refined, pi_server_name, prefix= None):
    """Essa função é executada para buscar os tags no PI Server e retornar um DataFrame.
    É possível filtrar os tags a partir do prefix com uma lista dos prefixos a serem ignorados.

    Args:
        client_ (obj): É o objeto de conexão com o PI Web API.
        tag_refined (str): Valor do tag a ser buscado (normalmente parte numérica).
        pi_server_name (str): Servido de conexão para acessar PI
        prefix (List, optional): Lista dos prefixos a serem ignorados, quando sabidos (execute primeiro e descubra quais são os prefixos que devem ser rejeitados da base e insira em lista no argumento). Defaults to None.
    """
    
    tag_name_to_find = f"*{tag_refined}*"
    piserver = client_.dataServer.get_by_name(pi_server_name)
    web_id_do_servidor = piserver.web_id

    points_encontrados = client.dataServer.get_points(
            web_id=web_id_do_servidor,
            name_filter=tag_name_to_find
        ).items

    #points_encontrados
    points_dictionary = [point.__dict__ for point in points_encontrados]

    df_points = pd.DataFrame(points_dictionary)
# Exemplo: remover tags cujos nomes começam com qualquer valor da lista de prefixos

    if prefix:
        df_points_filtrado = df_points[~df_points['_name'].str.startswith(tuple(prefixos))].reset_index(drop=True)
        return df_points_filtrado
    else:
        return df_points






#RESPOSTA ESPERADA
#_web_id     _id               _name                          _path  ...  _step _future                                             _links _web_exception
#0   F1DPNJgj2k8KS0GVnW4GF_MAnArywBAAUzI5NTBQSVNcTE...   76975        LIC_22311020        \\S2950PIS\LIC_22311020  ...  False   False  {'attributes': 'https://piwebapicorp.petrobras...           None    
#1   F1DPNJgj2k8KS0GVnW4GF_MAnAsiwBAAUzI5NTBQSVNcTE...   76978     LIC_22311020.ES     \\S2950PIS\LIC_22311020.ES  ...   True   False  {'attributes': 'https://piwebapicorp.petrobras...           None    
#2   F1DPNJgj2k8KS0GVnW4GF_MAnAsCwBAAUzI5NTBQSVNcTE...   76976     LIC_22311020.MV     \\S2950PIS\LIC_22311020.MV  ...  False   False  {'attributes': 'https://piwebapicorp.petrobras...           None    
#3   F1DPNJgj2k8KS0GVnW4GF_MAnAsSwBAAUzI5NTBQSVNcTE...   76977     LIC_22311020.SV     \\S2950PIS\LIC_22311020.SV  ...  False   False  {'attributes': 'https://piwebapicorp.petrobras...           None    
#4   F1DPNJgj2k8KS0GVnW4GF_MAnAZscBAAUzI5NTBQSVNcUF...  116582      PX_22311020.C1      \\S2950PIS\PX_22311020.C1  ...  False   False  {'attributes': 'https://piwebapicorp.petrobras...           None    
#5   F1DPNJgj2k8KS0GVnW4GF_MAnAZ8cBAAUzI5NTBQSVNcUF...  116583      PX_22311020.C2      \\S2950PIS\PX_22311020.C2  ...  False   False  {'attributes': 'https://piwebapicorp.petrobras...           None    
#6   F1DPNJgj2k8KS0GVnW4GF_MAnADKwBAAUzI5NTBQSVNcUF...  109580  PX_22311020.DEVPOS  \\S2950PIS\PX_22311020.DEVPOS  ...  False   False  {'attributes': 'https://piwebapicorp.petrobras...           None    
#7   F1DPNJgj2k8KS0GVnW4GF_MAnAM8gBAAUzI5NTBQSVNcUF...  116787    PX_22311020.DIFF    \\S2950PIS\PX_22311020.DIFF  ...  False   False  {'attributes': 'https://piwebapicorp.petrobras...           None    
#8   F1DPNJgj2k8KS0GVnW4GF_MAnAZMcBAAUzI5NTBQSVNcUF...  116580     PX_22311020.OUT     \\S2950PIS\PX_22311020.OUT  ...  False   False  {'attributes': 'https://piwebapicorp.petrobras...           None    
#9   F1DPNJgj2k8KS0GVnW4GF_MAnADawBAAUzI5NTBQSVNcUF...  109581   PX_22311020.PRESS   \\S2950PIS\PX_22311020.PRESS  ...  False   False  {'attributes': 'https://piwebapicorp.petrobras...           None    
#10  F1DPNJgj2k8KS0GVnW4GF_MAnAZccBAAUzI5NTBQSVNcUF...  116581      PX_22311020.RB      \\S2950PIS\PX_22311020.RB  ...  False   False  {'attributes': 'https://piwebapicorp.petrobras...           None    
#11  F1DPNJgj2k8KS0GVnW4GF_MAnADqwBAAUzI5NTBQSVNcUF...  109582    PX_22311020.TEMP    \\S2950PIS\PX_22311020.TEMP  ...  False   False  {'attributes': 'https://piwebapicorp.petrobras...           None


# Garante a criação do banco de dados
"""Base.metadata.create_all(engine)
for index, row in df_points_filtrado.iterrows():
    df_values = client.data.get_plot_values(f"pi:\\\\S2950PIS\\"  + row['_name'] , end_time="*", intervals=30000, start_time= "*-10y")
    # Convertendo a coluna 'Value' para numérica, forçando erros a NaN
    df_values['Timestamp'] = pd.to_datetime(df_values['Timestamp'], format="mixed")
    df_values['Value'] = pd.to_numeric(df_values['Value'], errors='coerce')
    df_values.dropna(subset=['Value'], inplace=True)
    df_values['value_sp'] = 0
    df_values['value_mv'] = 0
    df_values['tag'] = row['_name']
    df_values['description'] = row['_descriptor']
    df_values = df_values.rename(columns={'Value': 'value', 'Timestamp': 'timestamp'})
    dados_em_lote = df_values[['tag', 'description', 'value', 'timestamp']].to_dict(orient='records') #, 'value_sp', 'value_mv']].to_dict(orient='records')
    # Estabelece a sessão
    Session = sessionmaker(bind=engine)
    session = Session()

    session.bulk_insert_mappings(LeituraSensor, dados_em_lote)
    session.commit()
    resultados = session.query(LeituraSensor).all()
    for resultado in resultados:
        print(resultado)
    session.close()"""

# ==============================================================================
# 2. EXECUÇÃO DO PROCESSO
# ==============================================================================

# --- Parâmetros de execução ---
client = PIWebApiClient("https://piwebapicorp.petrobras.com.br/piwebapi/", useKerberos=True, verifySsl=False)
tag_refined = "22311020"
pi_server_name = "S2950PIS"
prefixos = ['FI_', 'PIC_', 'PT', 'TI_', 'LT_', 'PI_'] # Seus prefixos para ignorar

# --- Obtenha os tags do PI System usando sua função ---
print("Buscando tags no PI System...")
df_points_filtrado = get_tags(client, tag_refined, pi_server_name, prefixos)
print(f"Encontrados {len(df_points_filtrado)} tags após a filtragem.")
# print(df_points_filtrado[['_name', '_descriptor']].head()) # Descomente para depurar

# --- Configuração da Sessão com o Banco de Dados ---
Session = sessionmaker(bind=engine)
Base.metadata.create_all(engine) # Garante que as tabelas existam

# ==============================================================================
# 2.1. CADASTRA OS TAGS INICIO
# ==============================================================================

# Use sua função get_tags para obter o DataFrame completo
df_sensores_pi = get_tags(client, tag_refined, pi_server_name, prefixos)
print(f"Encontrados {len(df_sensores_pi)} sensores no PI System.")

if not df_sensores_pi.empty and False:
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # 2. VERIFICAR QUAIS TAGS JÁ EXISTEM NO BANCO
        tags_do_pi = df_sensores_pi['_name'].tolist()
        sensores_existentes_query = session.query(Sensores.tag).filter(Sensores.tag.in_(tags_do_pi))
        tags_existentes_no_db = {tag[0] for tag in sensores_existentes_query}
        print(f"{len(tags_existentes_no_db)} desses sensores já existem no banco de dados.")

        # 3. IDENTIFICAR SENSORES NOVOS E PREPARAR PARA INSERÇÃO
        sensores_para_adicionar = []
        for index, row in df_sensores_pi.iterrows():
            tag_name = row['_name']
            if tag_name not in tags_existentes_no_db:
                novo_sensor = Sensores(
                    # Usaremos o próprio nome do tag como ID, pois é único.
                    # Se seu modelo de dados exigir um ID diferente, ajuste aqui.
                    tag=tag_name,
                    description=row.get('_descriptor', None) # Pega a descrição se existir
                )
                sensores_para_adicionar.append(novo_sensor)
        
        # 4. INSERIR OS SENSORES NOVOS NO BANCO
        if sensores_para_adicionar:
            print(f"Adicionando {len(sensores_para_adicionar)} novos sensores ao banco de dados...")
            session.add_all(sensores_para_adicionar)
            session.commit()
            print("✅ Novos sensores cadastrados com sucesso!")
        else:
            print("Nenhum sensor novo para adicionar. O banco de dados já está sincronizado.")

    except Exception as e:
        print(f"❌ Ocorreu um erro: {e}")
        session.rollback()
    finally:
        session.close()


# ==============================================================================
# 2.1. CADASTRA OS TAGS FIM
# ==============================================================================
# --- Crie o mapa de TAG para ID a partir do seu banco de dados ---
print("\nCriando mapa de 'tag' para 'id' a partir do banco de dados local...")
mapa_tag_para_id = {}
with Session() as session:
    tags_a_consultar = df_points_filtrado['_name'].tolist()
    # Query para buscar sensores cujo 'tag' está na lista de nomes que veio do PI
    sensores_no_db = session.query(Sensores).filter(Sensores.tag.in_(tags_a_consultar)).all()
    mapa_tag_para_id = {sensor.tag: sensor.id for sensor in sensores_no_db}

print(f"Mapa criado. {len(mapa_tag_para_id)} dos tags encontrados no PI existem no banco de dados.")

# --- Loop principal para buscar dados e preparar para inserção ---
todos_os_dados_para_inserir = []
print("\nIniciando a coleta de dados de séries temporais...")

for index, row in df_points_filtrado.iterrows():
    tag_name = row['_name']
    
    # Pule tags que não estão no seu banco de dados local
    if tag_name not in mapa_tag_para_id:
        print(f"  -> AVISO: Tag '{tag_name}' não encontrado na tabela 'sensores'. Pulando.")
        continue

    try:
        print(f"Buscando dados para o tag: {tag_name}...")
        df_values = client.data.get_plot_values(
            f"pi:\\\\{pi_server_name}\\{tag_name}",
            end_time="*", intervals=30000, start_time="*-10y"
        )
        
        if df_values.empty:
            print(f"  -> Nenhum dado encontrado para {tag_name}.")
            continue

        # Limpeza e transformação dos dados
        df_values['Timestamp'] = pd.to_datetime(df_values['Timestamp'], format="mixed", errors='coerce')
        df_values['Value'] = pd.to_numeric(df_values['Value'], errors='coerce')
        df_values.dropna(subset=['Value', 'Timestamp'], inplace=True)
        
        # **AQUI ESTÁ A CORREÇÃO**: Use o mapa para obter o ID do sensor
        id_do_sensor = mapa_tag_para_id[tag_name]
        df_values['id_sensor'] = id_do_sensor
        
        df_values = df_values.rename(columns={'Value': 'value', 'Timestamp': 'timestamp'})
        
        dados_do_sensor_atual = df_values[['id_sensor', 'value', 'timestamp']].to_dict(orient='records')
        todos_os_dados_para_inserir.extend(dados_do_sensor_atual)
        print(f"  -> {len(dados_do_sensor_atual)} registros preparados para {tag_name}.")

    except Exception as e:
        print(f"  -> ERRO ao processar o tag {tag_name}: {e}")

# --- Inserção final em lote no banco de dados ---
if todos_os_dados_para_inserir:
    print(f"\nIniciando a inserção em lote de {len(todos_os_dados_para_inserir)} registros...")
    with Session() as session:
        try:
            session.bulk_insert_mappings(LeituraSensor, todos_os_dados_para_inserir)
            session.commit()
            print("✅ Inserção em lote concluída com sucesso!")
        except Exception as e:
            print(f"❌ Erro durante a inserção no banco de dados. Revertendo alterações.")
            print(f"   Erro: {e}")
            session.rollback()
else:
    print("\nNenhum dado novo para inserir no banco de dados.")