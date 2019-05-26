import errno
import os

import pandas as pd

from datasetreader.BaseReader import BaseReader


class CSVReader(BaseReader):
    """
    Faz a leitura do arquivo CSV e formatação dos dados para padronizar a saída.
    """

    # input fields => output fields
    # id, nome, email, telefone, valor_total, valor_com_desconto
    _FIELDS: dict = {
        'client_id': 'id',
        'username': 'nome',
        'email_client': 'email',
        'phone_client': 'telefone',
        'product_value': 'valor_total',
        'discount': 'desconto'
    }

    def __init__(self, csv_filename: str):
        """
        Inicializa o objeto com o nome do arquivo CSV que será feita a migração.

        :param csv_filename: nome do arquivo CSV.
        """

        super().__init__()

        self.csv_filename: str = csv_filename

    def load_data(self) -> None:
        """
        Faz a leitura do arquivo CSV com os dados dos usuários e carrega-os no dataframe de usuários.

        :raise FileNotFoundError: se o arquivo CSV não existir.
        :raise EmptyDataError: se não tiver conteúdo algum no arquivo.
        :raise ParserError: se ocorrer erro na leitura do arquivo CSV.
        """
        if os.path.isfile(self.csv_filename):
            self._df_user = pd.read_csv(self.csv_filename, sep=';')
        else:
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), self.csv_filename)
