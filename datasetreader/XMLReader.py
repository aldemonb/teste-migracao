import errno
import os

import pandas as pd
import xmltodict

from datasetreader.BaseReader import BaseReader


class XMLReader(BaseReader):
    """
    Faz a leitura do arquivo CSV e formatação dos dados para padronizar a saída.
    """

    # input fields => output fields
    # id, nome, email, telefone, valor_total, valor_com_desconto
    _FIELDS: dict = {
        'user_id': 'id',
        'name': 'nome',
        'email_user': 'email',
        'phone': 'telefone',
        'buy_value': 'valor_total'
    }

    def __init__(self, xml_filename: str):
        """
        Inicializa o objeto com o nome do arquivo XML que será feita a migração.

        :param xml_filename: nome do arquivo XML.
        """

        super().__init__()

        self.xml_filename: str = xml_filename

    def load_data(self) -> None:
        """
        Faz a leitura do arquivo XML com os dados dos usuários e carrega-os no dataframe de usuários.

        :raise FileNotFoundError: se o arquivo XML não existir.
        :raise ParsingInterrupted: se ocorrer alguma falha ao ler o arquivo XML.
        """
        if os.path.isfile(self.xml_filename):
            with open(self.xml_filename, 'r') as f:
                xml = ''.join(f.readlines())
            data = xmltodict.parse(xml)['records']['record']
            self._df_user = pd.DataFrame(data)
            self._df_user['buy_value'] = self._df_user['buy_value'].astype(float)
        else:
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), self.xml_filename)
