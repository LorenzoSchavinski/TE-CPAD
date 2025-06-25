import pandas as pd
from functools import reduce
import os

print(f"O script está sendo executado de: {os.getcwd()}")

# --- ETAPA 1: CONFIGURAÇÃO DE CAMINHOS E ARQUIVOS ---
caminho_microdados_enade = "C:/work/CPAD_TE/microdados_enade_2023/Microdados_Enade_2023/DADOS/"
caminho_planilhas_mec = "C:/work/CPAD_TE/"

# ATENÇÃO: É fundamental validar os nomes das colunas 'QE_' com o Dicionário de Variáveis do ENADE 2023.
colunas_e_tipos = {
    'NU_ANO': 'int16', 'CO_CURSO': 'int32', 'CO_IES': 'int32',
    'CO_CATEGAD': 'object', 'CO_MODALIDADE': 'object', 'CO_UF_CURSO': 'object',
    'CO_REGIAO_CURSO': 'object', 'CO_TURNO_GRADUACAO': 'object', 'NT_GER': 'float32',
    'NT_FG': 'float32', 'NT_CE': 'float32', 'TP_SEXO': 'object',
    'NU_IDADE': 'float32', 'QE_I04': 'object', 'QE_I05': 'object',
    'QE_I08': 'object', 'QE_I17': 'object',
}

arquivos_enade = [f"microdados2023_arq{i}.txt" for i in range(1, 33)]
dataframes_para_unir_enade = []
chaves_merge_enade_txt = ['NU_ANO', 'CO_CURSO']

# --- ETAPA 2: CARREGAR E UNIFICAR OS MICRODADOS DO ENADE 2023 ---
print("--- Carregando e Unificando Microdados ENADE 2023 ---")
for nome_arquivo in arquivos_enade:
    caminho_arquivo = os.path.join(caminho_microdados_enade, nome_arquivo)
    print(f"Carregando {nome_arquivo}...")
    try:
        header_df = pd.read_csv(caminho_arquivo, sep=';', nrows=0, encoding='ISO-8859-1', on_bad_lines='skip')
        todas_colunas_do_arquivo = set(header_df.columns)
        colunas_para_ler = [col for col in colunas_e_tipos.keys() if col in todas_colunas_do_arquivo]
        for key in chaves_merge_enade_txt:
            if key not in colunas_para_ler and key in todas_colunas_do_arquivo:
                colunas_para_ler.append(key)
        if not any(key in todas_colunas_do_arquivo for key in chaves_merge_enade_txt):
            continue
        dtypes_para_leitura = {k: v for k, v in colunas_e_tipos.items() if k in colunas_para_ler}
        df_temp = pd.read_csv(caminho_arquivo, sep=';', encoding='ISO-8859-1', usecols=colunas_para_ler, dtype=dtypes_para_leitura, on_bad_lines='skip')
        dataframes_para_unir_enade.append(df_temp)
    except Exception as e:
        print(f"Erro ao carregar {nome_arquivo}: {e}")

df_enade_completo = None
if dataframes_para_unir_enade:
    for i, df in enumerate(dataframes_para_unir_enade):
        if df.duplicated(subset=chaves_merge_enade_txt).any():
            dataframes_para_unir_enade[i] = df.drop_duplicates(subset=chaves_merge_enade_txt, keep='first')
    df_enade_completo = reduce(lambda left, right: pd.merge(left, right, on=chaves_merge_enade_txt, how='outer'), dataframes_para_unir_enade)
    print(f"\nUnificação do ENADE concluída. Shape: {df_enade_completo.shape}")
else:
    print("Nenhum DataFrame do ENADE foi carregado.")

# --- ETAPA 3: CARREGAR DADOS DO CENSO (MEC) E REMOVER DUPLICATAS ---
print("\n--- Carregando planilhas do MEC ---")
df_cadastro_cursos, df_ies = None, None
try:
    df_cadastro_cursos = pd.read_csv(os.path.join(caminho_planilhas_mec, 'MICRODADOS_CADASTRO_CURSOS_2023.CSV'), sep=';', encoding='ISO-8859-1', low_memory=False)
    print(f"Carregado MICRODADOS_CADASTRO_CURSOS_2023.CSV. Shape inicial: {df_cadastro_cursos.shape}")
    
    if 'TP_SITUACAO' in df_cadastro_cursos.columns:
        df_cadastro_cursos = df_cadastro_cursos[df_cadastro_cursos['TP_SITUACAO'] == 1]
    df_cadastro_cursos = df_cadastro_cursos.drop_duplicates(subset=['CO_CURSO'], keep='last')
    print(f"Shape do Cadastro de Cursos após remoção de duplicatas: {df_cadastro_cursos.shape}")

    df_ies = pd.read_csv(os.path.join(caminho_planilhas_mec, 'MICRODADOS_ED_SUP_IES_2023.CSV'), sep=';', encoding='ISO-8859-1', low_memory=False)
    print(f"Carregado MICRODADOS_ED_SUP_IES_2023.CSV. Shape: {df_ies.shape}")
except Exception as e:
    print(f"Erro ao carregar arquivos do Censo: {e}")

# --- ETAPA 4: INTEGRAR TODAS AS FONTES DE DADOS ---
df_final_completo = df_enade_completo
if all(df is not None for df in [df_final_completo, df_cadastro_cursos, df_ies]):
    print("\n--- Unificando ENADE com Cadastro de Cursos e IES ---")
    df_final_completo = pd.merge(df_final_completo, df_cadastro_cursos, on='CO_CURSO', how='left', suffixes=('_enade', '_curso_mec'))
    df_final_completo = pd.merge(df_final_completo, df_ies, left_on='CO_IES_enade', right_on='CO_IES', how='left', suffixes=('_curso', '_ies_mec'))
    print(f"Unificação completa. Shape final corrigido: {df_final_completo.shape}")

# --- ETAPA 5: FILTRAR PARA ÁREA DA SAÚDE ---
print("\n--- Filtrando para Área da Saúde ---")
if df_final_completo is not None and 'NO_CINE_AREA_GERAL' in df_final_completo.columns:
    area_saude = 'Saúde e bem-estar'
    df_final_completo.dropna(subset=['NO_CINE_AREA_GERAL'], inplace=True)
    df_final_completo = df_final_completo[df_final_completo['NO_CINE_AREA_GERAL'].str.contains(area_saude, case=False)].copy()
    print(f"Filtro aplicado. Registros da saúde: {len(df_final_completo)}")
else:
    print("Não foi possível aplicar o filtro de área da saúde.")

# --- ETAPA 6: PRÉ-PROCESSAMENTO E LIMPEZA DE DADOS ---
if df_final_completo is not None:
    print("\n--- Iniciando Pré-processamento ---")
    
    if 'CO_CATEGAD' in df_final_completo.columns:
        map_co_categad = {'1':'Pública Federal', '2':'Pública Estadual', '3':'Pública Municipal', '4':'Privada com fins lucrativos', '5':'Privada sem fins lucrativos', '7':'Especial'}
        df_final_completo['CO_CATEGAD_DESC'] = df_final_completo['CO_CATEGAD'].astype(str).str.split('.').str[0].map(map_co_categad).fillna('Não Informado')

    if 'QE_I08' in df_final_completo.columns:
        map_qe_i08_renda = {'A':'Até 1,5 s.m.', 'B':'De 1,5 a 3 s.m.', 'C':'De 3 a 4,5 s.m.', 'D':'De 4,5 a 6 s.m.', 'E':'De 6 a 10 s.m.', 'F':'De 10 a 30 s.m.', 'G':'Acima de 30 s.m.'}
        df_final_completo['QE_I08_DESC'] = df_final_completo['QE_I08'].astype(str).map(map_qe_i08_renda).fillna('Não Informado')

    colunas_numericas_para_imputar = ['NT_GER', 'NT_FG', 'NT_CE', 'NU_IDADE']
    for col in colunas_numericas_para_imputar:
        if col in df_final_completo.columns and df_final_completo[col].isnull().any():
            mediana_col = df_final_completo[col].median()
            df_final_completo[col] = df_final_completo[col].fillna(mediana_col)
    
    colunas_categoricas_para_imputar = ['TP_SEXO', 'CO_TURNO_GRADUACAO', 'QE_I04', 'QE_I05', 'QE_I08', 'QE_I17']
    for col in colunas_categoricas_para_imputar:
        if col in df_final_completo.columns and df_final_completo[col].isnull().any():
            moda_col = df_final_completo[col].mode()
            if not moda_col.empty:
                df_final_completo[col] = df_final_completo[col].fillna(moda_col[0])
    
    print("Pré-processamento concluído.")

# --- ETAPA 7: SELECIONAR COLUNAS FINAIS E SALVAR ---
if df_final_completo is not None:
    print("\n--- Selecionando colunas finais e salvando ---")
    colunas_para_manter = [
        'NT_GER', 'NT_FG', 'NT_CE',
        'NO_IES', 'CO_IES_enade',
        'NO_CURSO', 'NO_CINE_ROTULO',
        'NO_CINE_AREA_GERAL',
        'CO_CATEGAD_DESC',
        'SG_UF_IES', 'NO_REGIAO_IES',
        'TP_ORGANIZACAO_ACADEMICA_ies_mec',
        'QE_I08_DESC',
        'QE_I04', 'QE_I05', 'QE_I17',
        'CO_MODALIDADE', 'CO_TURNO_GRADUACAO'
    ]
    colunas_existentes = [col for col in colunas_para_manter if col in df_final_completo.columns]
    df_focado = df_final_completo[colunas_existentes].copy()
    print(f"Seleção concluída. DataFrame final com {df_focado.shape[1]} colunas.")

    nome_arquivo_saida = 'dataset_final.csv'
    caminho_saida = os.path.join(caminho_planilhas_mec, nome_arquivo_saida)
    try:
        df_focado.to_csv(caminho_saida, index=False, sep=';', encoding='utf-8-sig')
        print(f"\nArquivo salvo com SUCESSO!")
        print(f"Nome do arquivo: {nome_arquivo_saida}")
        print(f"Localização: {caminho_saida}")
    except Exception as e:
        print(f"\nOcorreu um erro ao salvar o arquivo: {e}")
else:
    print("\nNenhum DataFrame final foi gerado para salvar.")

print("\n--- Script Concluído ---")