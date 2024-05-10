from pathlib import Path
import pandas as pd
import logging


# Constantes
ROOT_PATH = Path(__file__).parents[1]
# Configuração de logging
logging.basicConfig(level=logging.INFO)


class Atividade:
    """
    Classe criando uma atividade de um atleta amador
    """
    def __init__(
        self,
        name,
        distance,
        moving_time,
        elapsed_time,
        total_elevation_gain,
        sport_type,
        id,
        start_date_local,
        achievement_count,
        kudos_count,
        comment_count,
        photo_count,
        average_speed,
        max_speed,
        average_heartrate,
        max_heartrate,
        elev_high,
        elev_low,
        pr_count
    ):
        self.name = name
        self.distance = distance
        self.moving_time = moving_time
        self.elapsed_time = elapsed_time
        self.total_elevation_gain = total_elevation_gain
        self.sport_type = sport_type
        self.id = id
        self.start_date_local = start_date_local
        self.achievement_count = achievement_count
        self.kudos_count = kudos_count
        self.comment_count = comment_count
        self.photo_count = photo_count
        self.average_speed = average_speed
        self.max_speed = max_speed
        self.average_heartrate = average_heartrate
        self.max_heartrate = max_heartrate
        self.elev_high = elev_high
        self.elev_low = elev_low
        self.pr_count = pr_count

    @classmethod
    def converter_distancia(cls, df):
        """
        Converter m -> km
        """
        df["distance"] = df['distance'] / 1000
        logging.info("Distância convertida!")

        return df

    @classmethod
    def converter_tempo(cls, df):
        """
        Converter s -> HH:MM:SS
        """
        df['moving_time'] = pd.to_timedelta(df['moving_time'], unit='s')
        df['elapsed_time'] = pd.to_timedelta(df['elapsed_time'], unit='s')
        logging.info("Tempo convertido!")

        return df

    @classmethod
    def converter_velocidade(cls, df):
        """
        Converter m/s -> km/h
        """
        df['average_speed'] = df['average_speed'] * 3.6
        df['max_speed'] = df['max_speed'] * 3.6
        logging.info("Velocidade convertida!")

        return df


class GerenciadorAtividades:
    """
    Classe que vai manipular as atividades
    """
    def __init__(self, arquivo):
        self.df = pd.read_csv(arquivo, encoding="utf-8")
        self.preparar_dados()

    def preparar_dados(self):
        """
        ### Conversões iniciais ###
        dt_hr_inicio para datetime
        distancia: m -> km
        tempo_mov / elapsed_time: s -> hh:mm:ss
        velocidade_media / velocidade_maxima: m/s -> km/h
        Extraindo apenas dados a partir de 2021
        Criando a coluna Data contendo apenas a data da atividade
        Criando a coluna activity_duration contendo a duração da atividade
        Setando a coluna idt como índice
        """
        self.df["start_date_local"] = (
            pd.to_datetime(self.df["start_date_local"])
        )
        self.df = Atividade.converter_distancia(self.df)
        self.df = Atividade.converter_tempo(self.df)
        self.df = Atividade.converter_velocidade(self.df)
        self.df = self.df[self.df["start_date_local"] > "2021-01-01"]
        self.df["data"] = pd.to_datetime(self.df["start_date_local"].dt.date)
        self.df['end_date_local'] = (
            self.df['start_date_local'] + self.df['elapsed_time']
        )
        self.df['activity_duration'] = (
            self.df['end_date_local'] - self.df['start_date_local']
        )
        self.df.set_index("id", inplace=True)

        logging.info("Dados preparados!")

    def verifica_nulos(self):
        """
        Verificando presença de valores nulos nos dados
        """
        nulos_colunas = self.df.isnull().sum()
        percentual_nulos = (nulos_colunas / len(self.df)) * 100
        nulos = {col: val for col, val in nulos_colunas.items() if val > 0}
        percentual = {
            col: val for col, val in percentual_nulos.items() if val > 0
            }
        logging.info(f"Nulos: {nulos}\nPercentual de nulos: {percentual}")

    def preenche_nulos(self, indice, coluna, valor):
        """
        Preenche os valores nulos de uma coluna esecífica com o valor fornecido
        """
        self.df.loc[indice, coluna] = valor

    def trata_nulos(self):
        """
        Atividades indoor: WeightTraining, Workout
            Não há elevação
        Atividades ao ar livre: Run, Walk
            Elevação = média da coluna
        """
        atividades_indoor = ["WeightTraining", "Workout"]
        for atividade in atividades_indoor:
            indices = self.df[self.df["sport_type"] == atividade].index
            self.preenche_nulos(indices, "elev_high", 0)
            self.preenche_nulos(indices, "elev_low", 0)

        # Run setar para a média das demais atividades
        avg_elev_high = self.df["elev_high"].mean()
        avg_elev_low = self.df["elev_low"].mean()

        indices = self.df[self.df["sport_type"] == "Run"].index
        self.preenche_nulos(indices, "elev_high", avg_elev_high)
        self.preenche_nulos(indices, "elev_low", avg_elev_low)

        # Preenchimento de nulos para frequência cardíaca com a média
        avg_heart = self.df["average_heartrate"].mean()
        self.df["average_heartrate"].fillna(avg_heart, inplace=True)
        self.df["max_heartrate"].fillna(avg_heart, inplace=True)

        logging.info("Nulos tratados!")

    def salvar_dados(self, caminho):
        """
        Salvando arquivo pronto para análise
        """
        if caminho is None:
            caminho = ROOT_PATH / "data/processed/data.csv"

        self.df.to_csv(caminho, index=False, header=True)
        logging.info("Arquivo data.csv salvo!")


if __name__ == "__main__":

    # Ler do arquivo strava_data.csv
    caminho_ler = ROOT_PATH / "data/raw/strava_data.csv"
    caminho_salvar = ROOT_PATH / "data/processed/data.csv"

    if not caminho_ler.is_file():
        logging.warning("O arquivo strava_data.csv não foi encontrado!\
 Por favor colete os dados do Strava novamente!")
    else:
        dados_atividade = GerenciadorAtividades(caminho_ler)
        dados_atividade.verifica_nulos()
        dados_atividade.trata_nulos()
        dados_atividade.salvar_dados(caminho_salvar)
