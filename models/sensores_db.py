import datetime
from sqlalchemy import create_engine, Column, Integer, Float, DateTime, String
from sqlalchemy.orm import declarative_base, sessionmaker
DATABASE_FILE = "dados_sensores.db"
engine = create_engine(f'sqlite:///{DATABASE_FILE}')

Base = declarative_base()

#DEFINIÇÃO DA MODEL ORM
# -----------------------------------------------------------------------------

class LeituraSensor(Base):
    """
    Representa uma única leitura de um sensor em um determinado momento.
    """
    __tablename__ = 'leituras_sensores' # Nome da tabela no banco de dados

    # Colunas da tabela
    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, default=None)
    value_pv = Column(Float, nullable=False) # Process variable
    value_sp = Column(Float, nullable=False) # Setpoint
    value_mv = Column(Float, nullable=False) # Manipulated variable
    
    
    def __repr__(self):
        """Representação em string do objeto, útil para debug."""
        return (f"<LeituraSensor(id={self.id}, timestamp='{self.timestamp}', "
            f"value_pv={self.value_pv}, value_sp={self.value_sp}, value_mv={self.value_mv})>")
    
Base.metadata.create_all(engine)

#Exemplo de Como Usar o Modelo
if __name__ == '__main__':
    # Cria uma "fábrica" de sessões para interagir com o banco
    Session = sessionmaker(bind=engine)
    session = Session()

    # Cria um novo objeto (uma nova linha para a tabela)
    # OBS: O valor com vírgula (60,08304) deve ser convertido para float (60.08304)
    nova_leitura = LeituraSensor(
        timestamp=datetime.datetime(2015, 10, 1, 14, 30, 0), # Exemplo de data e hora
        valor=60.08304
    )

    # Adiciona o objeto à sessão
    session.add(nova_leitura)

    # Salva (comita) as mudanças no banco de dados
    session.commit()

    print(f"\nNova leitura adicionada: {nova_leitura}")

    # Exemplo de como buscar todas as leituras no banco
    print("\nBuscando todas as leituras salvas:")
    todas_as_leituras = session.query(LeituraSensor).all()
    for leitura in todas_as_leituras:
        print(leitura)
        
    # Fecha a sessão
    session.close()