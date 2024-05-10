import requests
import pandas as pd
from pathlib import Path


# Constantes
AUTH_URL = "https://www.strava.com/oauth/token"
ACTIVITIES_URL = "https://www.strava.com/api/v3/athlete/activities"
ROOT_PATH = (Path(__file__).parent).parent


class Conexao:
    """
    Classe que cria a conexao, extrai e salva os dados brutos
    """
    def __init__(self, cliente_id, cliente_secret, refresh_token):
        self._cliente_id = cliente_id
        self._cliente_secret = cliente_secret
        self._refresh_token = refresh_token
        self.atividades = []
        self.num_pagina = 1

    @property
    def cliente_id(self):
        return self._cliente_id

    @property
    def cliente_secret(self):
        return self._cliente_secret

    @property
    def refresh_token(self):
        return self._refresh_token

    def _autenticar(self):
        """
        Dados e processo para a autenticação
        """
        autenticacao = {
            "client_id": self.cliente_id,
            "client_secret": self.cliente_secret,
            "refresh_token": self.refresh_token,
            "grant_type": "refresh_token",
            "f": "json",
        }

        with requests.post(AUTH_URL, autenticacao, False) as resp:
            resp.raise_for_status()
            return resp.json()["access_token"]

    def _recuperar_atividades(self, access_token):
        """
        Caso seja autenticado, é iniciado a recuperação
        dos dados de atividades do usuário.
        Armazenado db no final da lista atividades
        Incrementado o número da página
        """
        head = {"Authorization": "Bearer " + access_token}

        while True:
            param = {"per_page": 200, "page": self.num_pagina}
            db = requests.get(ACTIVITIES_URL, headers=head, params=param)
            db = db.json()

            if not db:
                break

            self.atividades.extend(db)
            self.num_pagina += 1

    def _salvar_dados(self):
        """
        Os dados armazenados em atividades são transformados em
        uma DataFrame e armazenado em um arquivo csv.
        """
        df = pd.DataFrame(self.atividades)

        """
        Filtrar as colunas a serem utilizadas
        name: O nome dado à atividade pelo usuário.
        distance: A distância total percorrida durante a atividade, geralmente
        em metros.
        moving_time: O tempo total em movimento, excluindo as paradas,
        geralmente em segundos.
        elapse_time: O tempo total decorrido desde o início até o fim da
        atividade, incluindo todas as paradas, geralmente em segundos.
        total_elevation_gain: O ganho total de elevação durante a atividade,
        geralmente em metros.
        sport_type: Um campo mais específico que detalha o tipo de esporte
        dentro de uma categoria de atividade.
        id: Um identificador único para a atividade.
        start_date_local: A data e hora de início da atividade.
        achievement_count: O número de conquistas ganhas durante a atividade.
        kudos_count: O número curtidas que a atividade recebeu.
        comment_count: O número de comentários feitos na atividade.
        photo_count: O número de fotos associadas à atividade.
        average_speed: A velocidade média durante a atividade, geralmente em
        metros por segundo.
        max_speed: A velocidade máxima atingida durante a atividade.
        average_heartrate: A frequência cardíaca média durante a atividade.
        max_heartrate: A frequência cardíaca máxima alcançada durante a
        atividade.
        elev_high: A maior elevação alcançada durante a atividade.
        elev_low: A menor elevação durante a atividade.
        pr_count: O número de recordes pessoais alcançados durante a atividade.
        """
        colunas_utilizadas = [
            "name",
            "distance",
            "moving_time",
            "elapsed_time",
            "total_elevation_gain",
            "sport_type",
            "id",
            "start_date_local",
            "achievement_count",
            "kudos_count",
            "comment_count",
            "photo_count",
            "average_speed",
            "max_speed",
            "average_heartrate",
            "max_heartrate",
            "elev_high",
            "elev_low",
            "pr_count"
        ]
        df = df[colunas_utilizadas]

        caminho = ROOT_PATH / "data/raw/strava_data.csv"
        df.to_csv(caminho, index=False, header=True)

    def criar_conexao(self):
        """
        Criando conexão,
        Autenticando,
        Recuperando atividades,
        Verificando se já esta salva,
        Salvando os dados.
        """
        try:
            access_token = self._autenticar()
            self._recuperar_atividades(access_token)

            if self.atividades:
                self._salvar_dados()
                print("Dados salvos em Strava_Data!\n")

        except requests.exceptions.HTTPError as err:
            print(f"Erro: {err}")


if __name__ == "__main__":
    """
    Abrir arquivo onde está as chaves para acesso à API.
    Armazenado em variáveis e passado ao objeto Conexao
    """
    arquivo = input("Digite o caminho do arquivo: ")

    with open(arquivo, "r") as f:
        c_id, c_secret, refresh_token = f.read().replace("'", "").split("\n")
        Conexao(c_id, c_secret, refresh_token).criar_conexao()
