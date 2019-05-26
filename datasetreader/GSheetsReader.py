import errno
import os
import pickle
from typing import Optional

import pandas as pd
# noinspection PyPackageRequirements
from google.auth.transport.requests import Request
# noinspection PyPackageRequirements
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
# noinspection PyPackageRequirements
from googleapiclient.discovery import build

from datasetreader.BaseReader import BaseReader


class GSheetsReader(BaseReader):
    """
    Faz a leitura do arquivo CSV e formatação dos dados para padronizar a saída.
    """

    # input fields => output fields
    # id, nome, email, telefone, valor_total, valor_com_desconto
    _FIELDS: dict = {
        'id': 'id',
        'nome': 'nome',
        'email': 'email',
        'telefone': 'telefone',
        'valor': 'valor_total',
        'desconto': 'desconto'
    }

    # input fields => output fields
    # id, usuario_id, dependente_de_id;
    _FIELDS_DEPENDANT: dict = {
        'id': 'id',
        'user_id': 'usuario_id',
        'dependente_id': 'dependente_de_id',
        'data_hora': 'data_hora'
    }

    _GSHEETS_SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

    def __init__(self, spreadsheet_id: str, page_users: str = 'usuarios', page_dependants: str = 'dependentes',
                 credentials_file: str = 'credentials.json'):
        """
        Inicializa o objeto com o nome do arquivo CSV que será feita a migração.

        :param spreadsheet_id: identificador da planilha no Google Sheets.
        :param page_users: nome da página com os dados dos usuários na planilha (padrão: usuários).
        :param page_dependants: nome da página com os dados dos dependentes na planilha (padrão: dependentes).
        :param credentials_file: arquivo no format JSON com as configurações de credenciais de acesso às planilhas.
        """

        super().__init__()

        self.spreadsheet_id: str = spreadsheet_id
        self.page_users: str = page_users
        self.page_dependants: str = page_dependants
        self.credentials_file: str = credentials_file

    # Código extraído de https://developers.google.com/sheets/api/quickstart/python
    def load_credentials(self) -> Optional[Credentials]:
        """
        Solicita as credenciais de acesso ao Google Sheets e armazena a autorização para uso futuro.

        :return: credenciais para acesso ou None caso não consiga recuperar as credenciais.
        """

        credentials: Optional[Credentials] = None

        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                credentials = pickle.load(token)

        # If there are no (valid) credentials available, let the user log in.
        if not credentials or not credentials.valid:

            if credentials and credentials.expired and credentials.refresh_token:
                credentials.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(self.credentials_file, self._GSHEETS_SCOPES)
                credentials = flow.run_local_server()

            # Save the credentials for the next run
            with open('token.pickle', 'wb') as token:
                pickle.dump(credentials, token)

        return credentials

    def load_data(self) -> None:
        """
        Faz a leitura da planilha no Google Sheets com os dados dos usuários e dos dependentes e carrega esses dados
        no dataframe de usuários e de dependentes, respectivamente.

        :raise FileNotFoundError: se o arquivo de configuração com as credenciais não existir.
        """

        if os.path.isfile(self.credentials_file):

            credentials = self.load_credentials()
            if credentials is None:
                raise ValueError('Não foi possível opter permissão para acesso à planilha no Google Sheets.')

            # Código extraído de https://developers.google.com/sheets/api/quickstart/python

            # Call the Sheets API
            service = build('sheets', 'v4', credentials=credentials)
            sheet = service.spreadsheets()

            # primeira linha é cabeçalho
            values = sheet.values().get(spreadsheetId=self.spreadsheet_id, range=self.page_users).execute().get(
                'values', [])
            self._df_user = pd.DataFrame(values[1:], columns=values[0])

            # primeira linha é cabeçalho
            values = sheet.values().get(spreadsheetId=self.spreadsheet_id, range=self.page_dependants).execute().get(
                'values', [])
            self._df_dependant = pd.DataFrame(values[1:], columns=values[0])

        else:
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), self.credentials_file)
