#1. Ler dados do CSV
#Ler os dados de um arquivo CSV e imprimir na tela
import pandas as pd

df = pd.read_csv('leituras_padronizadas_imputed.csv')

# Converter para datetime e definir índice
df['timestamp_group'] = pd.to_datetime(df['timestamp_group'])
df = df.set_index('timestamp_group')
# Garantir a ordem cronológica
df = df.sort_index()

# Selecionar colunas específicas (Inicialmente as colunas que possuem dados completos)
colunas_para_manter = [
    'LIC_22311020', 
    'LIC_22311020.ES', 
    'LIC_22311020.MV', 
    'LIC_22311020.SV', 
    'target'
]

df_limpo = df[colunas_para_manter]

df_limpo

#2. Engenharia de Features Temporais
# 2.1. Features de Lag (Atraso) valor do sensor X minutos atrás
# 2.2. Features de Janela Móvel (Rolling Window) média, desvio padrão, min, max do sensor nos últimos Y minutos

# Snippet de exemplo para esta etapa
df_features = df_limpo.copy()

# Vamos pegar todas as colunas de sensores (tudo, exceto 'target')
variaveis_sensores = [col for col in df_limpo.columns if col != 'target']

# PERÍODOS BASEADOS EM NÚMERO DE LINHAS (amostras)
# Em vez de '5T' (5 minutos), usamos '5' (5 linhas)
# Vamos usar as 5, 10 e 20 amostras anteriores para criar features.
periodos_janela_int = [5, 10, 20] 

print("Iniciando Engenharia de Features...")
for sensor in variaveis_sensores:
    for n_rows in periodos_janela_int:
        
        # Criar Lags (valor N linhas atrás)
        df_features[f'{sensor}_lag_{n_rows}'] = df_features[sensor].shift(n_rows)
        
        # Criar Média Móvel (baseada nas N linhas anteriores)
        df_features[f'{sensor}_roll_mean_{n_rows}'] = df_features[sensor].rolling(window=n_rows).mean()
        
        # Criar Desvio Padrão Móvel
        df_features[f'{sensor}_roll_std_{n_rows}'] = df_features[sensor].rolling(window=n_rows).std()

print("Engenharia de Features concluída.")

#3. Preparação Final
# 3.1. Remover NaNs
# 3.2. Definir X e y

# Snippet de exemplo para esta etapa

# 1. Remover NaNs criados pela engenharia de features
df_final = df_features.dropna()

# 2. Definir X e y
y = df_final['target']
X = df_final.drop('target', axis=1)

#4. Divisão de Treino e Teste
# Os dados deverão seguir ordem cronológica

# Snippet de exemplo para esta etapa
tamanho_teste_percent = 0.20 # Usar 20% dos dados para teste
tamanho_teste_idx = int(len(X) * (1 - tamanho_teste_percent))

X_train, X_test = X.iloc[:tamanho_teste_idx], X.iloc[tamanho_teste_idx:]
y_train, y_test = y.iloc[:tamanho_teste_idx], y.iloc[tamanho_teste_idx:]

print(f"Tamanho Treino: {len(X_train)} amostras")
print(f"Tamanho Teste:  {len(X_test)} amostras")

#5
from xgboost import XGBClassifier

# Calcular o scale_pos_weight (MUITO IMPORTANTE para dados desbalanceados)
contagem_0 = y_train.value_counts()[0.0]
contagem_1 = y_train.value_counts()[1.0]
escala = contagem_0 / contagem_1 
print(f"Usando scale_pos_weight: {escala:.2f}")

# Instanciar o modelo de CLASSIFICAÇÃO
xgb_class = XGBClassifier(
    n_estimators=100,      
    learning_rate=0.1,
    objective='binary:logistic', # Objetivo de classificação
    scale_pos_weight=escala,     # Parâmetro para desbalanceamento
    use_label_encoder=False,
    eval_metric='logloss'        
)

# Treinar o modelo
xgb_class.fit(X_train, y_train)

#6 
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
import seaborn as sns

# Fazer previsões nos dados de teste
y_pred = xgb_class.predict(X_test)

# Avaliar
print("Acurácia:", accuracy_score(y_test, y_pred))
print("\nMatriz de Confusão:")
# Mostrar matriz de confusão como gráfico
cm = confusion_matrix(y_test, y_pred)
print(cm)
import matplotlib.pyplot as plt

plt.figure(figsize=(6,5))
sns.heatmap(cm, annot=True, fmt='d', cmap='Blues',
            xticklabels=['Pred: Classe 0 (Normal)', 'Pred: Classe 1 (Falha)'],
            yticklabels=['True: Classe 0 (Normal)', 'True: Classe 1 (Falha)'])
plt.ylabel('Verdadeiro')
plt.xlabel('Predito')
plt.title('Matriz de Confusão')
plt.show()
print("\nRelatório de Classificação:")
print(classification_report(y_test, y_pred, target_names=['Classe 0 (Normal)', 'Classe 1 (Falha)']))