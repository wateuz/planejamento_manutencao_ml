#Execução de consulta máxima para todos tags escolhidos
import pandas as pd
import datetime
from sqlalchemy.orm import sessionmaker
from models.sensores_db import LeituraSensor, engine, Base

# Garante a criação do banco de dados
Base.metadata.create_all(engine)
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
    session.close()