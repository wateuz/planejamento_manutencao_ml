import pandas as pd
import datetime
from sqlalchemy.orm import sessionmaker
from models.sensores_db import LeituraSensor, Sensores, engine, Base
from sqlalchemy.orm import joinedload
import time
from datetime import timedelta


def padronizar_leituras():
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Obter todas as leituras de sensores
        # carregar leituras junto com os sensores (ajuste o nome da relação se for diferente)
        leituras = (
            session.query(LeituraSensor)
            .join(LeituraSensor.sensor)  # garante que dá para ordenar por campos do sensor
            .options(joinedload(LeituraSensor.sensor))
            .order_by(LeituraSensor.timestamp.asc(), Sensores.tag.asc())
            .all()
        )
        df_standardized = pd.DataFrame()
        threshold_group = 10 # considerando leituras com até 10 minutos de diferença como parte do mesmo grupo
        threshold_between = 30 # Considerando leituras com mais de 30 minutos de diferença como pertencentes a grupos diferentes
        last_time_stamp = None
        records = []
        for leitura in leituras:
            delta = leitura.timestamp - last_time_stamp if last_time_stamp else timedelta(minutes=threshold_between + 1)
            #leitura (objeto leitura) usar leitura.value, leitura.timestamp, etc.
            #Sensor (objeto relacionado) usar sensor.tag, sensor.description, etc.
            sensor = leitura.sensor
            if delta >= timedelta(minutes=threshold_between) or last_time_stamp is None:
                first_time_stamp = leitura.timestamp
                last_time_stamp = leitura.timestamp
                print(" Diferença maior que o threshold_between. Novo grupo de leituras.")
                #time.sleep(5)
            #============== DEBUG ==================
            if last_time_stamp:
                
                print(f"Delta Time: {leitura.timestamp - last_time_stamp}")
                delta = leitura.timestamp - last_time_stamp # diferença entre timestamps anterios e atual
                
                print(f" Delta está dentro do threshold? {delta <= timedelta(minutes=threshold_group)}")
                if delta <= timedelta(minutes=threshold_group):
                    print("Mesmo grupo de leituras.")
                    
                    records.append({
                    "id": leitura.id,
                    "sensor_tag": sensor.tag,
                    "sensor_description": getattr(sensor, "description", None),
                    "timestamp": leitura.timestamp,
                    "value": leitura.value,
                        "timestamp_group": first_time_stamp
                    })   
                    print(f"Adicionada leitura ao grupo com timestamp {first_time_stamp}.")       
                elif delta >= timedelta(minutes=threshold_between):
                    print("Diferença maior que o threshold_between. Novo grupo de leituras.")
                    #first_time_stamp = leitura.timestamp
                    records.append({
                        "id": leitura.id,
                        "sensor_tag": sensor.tag,
                        "sensor_description": getattr(sensor, "description", None),
                        "timestamp": leitura.timestamp,
                        "value": leitura.value,
                        "timestamp_group": first_time_stamp
                    })
            else:
                print("Primeira leitura do conjunto.")
                records.append({
                    "id": leitura.id,
                    "sensor_tag": sensor.tag,
                    "sensor_description": getattr(sensor, "description", None),
                    "timestamp": leitura.timestamp,
                    "value": leitura.value,
                    "timestamp_group": first_time_stamp
                })
                     
        df_standardized = pd.DataFrame(records)  
        print(df_standardized)  
            
            #print(leitura.value, leitura.timestamp)
            #print(sensor.tag, sensor.description)
            #print(f"{sensor.tag} - {leitura.timestamp} - {leitura.value}")
        return df_standardized
    except Exception as e:
        print(f"Erro ao padronizar leituras: {e}")
        return False
    finally:
        session.close()
    
    
df = padronizar_leituras()



AGREGAÇÃO = 'mean'

df_pivot = df.pivot_table(
    index='timestamp_group',  # Novo índice (o timestamp padronizado)
    columns='sensor_tag',             # Novas colunas (as tags)
    values='value',            # Valores que preencherão as células
    aggfunc=AGREGAÇÃO          # Como lidar com duplicatas
)
print(df_pivot)
df_pivot.to_excel("leituras_padronizadas_pivot.xlsx", index=False)
#agora = datetime.datetime.now()
#print(agora - timedelta(hours=3))
#threshold_minutes = 5
#print(timedelta(minutes=threshold_minutes))