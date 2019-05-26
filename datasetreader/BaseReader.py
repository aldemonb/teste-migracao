from abc import ABC, abstractmethod
from typing import Optional

import pandas as pd
import phonenumbers
from pandas.core.dtypes.common import is_numeric_dtype


class BaseReader(ABC):
    """
    Classe abstrata usada para padronizar os dados de saída de usuários e dependentes, a fim de facilitar a
    migração de informações a partir de várias origens diferentes.

    A classe que herdar a BaseReader deve sobrepor o método `load_data` e armazenar os dados de origem nas
    propriedades `_df_user` e `_df_dependant`, este opcional. Caso não informe dados na propriedade `_df_dependant`,
    a padronização de dependentes será ignorada.

    Exemplo simples de uso de classe herdada:
     reader = BaseReaderInherited('param1', 'param2', 'param_n')

     reader.do_process()

     print(reader.get_user_as_csv())

     print(reader.get_dependant_as_dict())
    """

    def __init__(self):
        """
        Define as variáveis de instâncias do objeto: dataframe de usuários e de dependentes.
        """

        self._df_user: Optional[pd.DataFrame] = None
        self._df_dependant: Optional[pd.DataFrame] = None

    @abstractmethod
    def load_data(self) -> None:
        """
        Essa função abstrata deve ser sobreposta pela classe herdada para realizar a leitura dos dados na origem
        e carregá-los nos dataframes de usuários e de dependentes. O dataframe de dependentes é opcional, pois
        existem algumas fontes que só tem dados de usuários.
        """
        pass

    @property
    def _fields(self) -> dict:
        """
        Recupera a lista de nomes das colunas de usuário no arquivo de origem com o respectivo nome padronizado.
        A classe herdada precisa definir a propriedade _FIELDS no formato dict.

        Os nomes padronizados são: id, nome, email, telefone, valor_total e valor_com_desconto.

        :return: lista de nomes das colunas de usuário com respectivo nome padronizado
        """

        return getattr(self, '_FIELDS')

    @property
    def _fields_dependant(self) -> dict:
        """
        Recupera a lista de nomes das colunas de dependentes no arquivo de origem com o respectivo nome padronizado.
        A classe herdada precisa definir a propriedade _FIELDS_DEPENDANT no formato dict.

        Os nomes padronizados são: id, usuario_id, dependente_de_id e data_hora.

        :return: lista de nomes das colunas de usuário com respectivo nome padronizado
        """

        return getattr(self, '_FIELDS_DEPENDANT', None)

    def prepare_data(self) -> None:
        """
        Padroniza o nome das colunas de usuário e dependentes e verifica se está faltando alguma coluna obrigatória.
        """

        self._df_user.rename(columns=self._fields, inplace=True)

        diff_columns = set(self._fields.values()) - set(self._df_user.columns) - {'valor_com_desconto'}
        if len(diff_columns) > 0:
            raise KeyError(f'As seguintes colunas estão faltando nos dados dos usuários: {diff_columns}')

        if self._df_dependant is not None and self._fields_dependant is not None:
            self._df_dependant.rename(columns=self._fields_dependant, inplace=True)

            diff_columns = set(self._fields_dependant.values()) - set(self._df_dependant.columns)
            if len(diff_columns) > 0:
                raise KeyError(f'As seguintes colunas estão faltando nos dados dos dependentes: {diff_columns}')

    def process_data(self) -> None:
        """
        Faz a conversão dos dados e formatações de acordo com requisitos pré-definidos:
         - quando os dados estivem em branco, importar em branco;
         - o telefone deve ser +55DDDNUMERO. Ex: (+5516981773421);
         - o Valor deve ser formatado como dinheiro (real). Ex: 999,00;
         - o valor_com_desconto deve ser calculado com o valor_total - desconto%;
         - datas no formato TIMESTAMP;
        """

        # O telefone deve ser +55DDDNUMERO. Ex: (+5516981773421);
        def format_phone(x: str) -> str:
            # noinspection PyBroadException
            if len(x.strip()) > 0:
                x = phonenumbers.format_number(phonenumbers.parse(x, 'BR'), phonenumbers.PhoneNumberFormat.E164)
            return x
        self._df_user['telefone'] = self._df_user['telefone'].apply(format_phone)

        # O valor_com_desconto deve ser calculado com o valor_total - desconto%;
        if 'desconto' in self._df_user.columns:

            if not is_numeric_dtype(self._df_user['desconto']):
                self._df_user['desconto'].replace(to_replace={'-': 0}, inplace=True)
                self._df_user["desconto"] = self._df_user["desconto"].apply(pd.to_numeric)

            if not is_numeric_dtype(self._df_user['valor_total']):
                self._df_user['valor_total'].replace(to_replace={'R\\$': '', ',': '.'}, regex=True, inplace=True)
                self._df_user["valor_total"] = self._df_user["valor_total"].apply(pd.to_numeric)

            self._df_user['desconto'] = self._df_user['valor_total'] * (1 - self._df_user['desconto'] / 100)
            self._df_user.rename(columns={'desconto': 'valor_com_desconto'}, inplace=True)

        else:

            self._df_user['valor_com_desconto'] = self._df_user['valor_total']

        # O Valor deve ser formatado como dinheiro (real). Ex: 999,00;
        for column in ['valor_total', 'valor_com_desconto']:
            self._df_user[column] = self._df_user[column].apply(lambda x: f'{x:.2f}'.replace('.', ','))

        if self._df_dependant is not None:

            # Datas no formato TIMESTAMP;
            self._df_dependant['data_hora'] = pd.to_datetime(self._df_dependant['data_hora']).dt.strftime(
                "%Y-%m-%d %H:%M:%S").replace({'NaT': ''})

            # Se forem usar unix timestamp, descomentar as seguintes linhas:
            # self._df_dependant['data_hora'] = (
            #         (pd.to_datetime(self._df_dependant['data_hora']) - pd.Timestamp('1970-01-01')
            #          ) // pd.Timedelta('1s')).astype(str).replace({'nan': '', '\\.0$': ''}, regex=True)

    def get_user_as_dict(self) -> dict:
        """
        recupera os dados processados dos usuários no formato dict

        :return: os dados no formato dict
        """

        return self._df_user.to_dict('r')

    def get_user_as_rows(self) -> any:
        """
        recupera os dados processados dos usuários no formato de vetores

        :return: os dados no formato de vetores
        """

        return self._df_user.to_records(index=False)

    def get_user_as_csv(self) -> str:
        """
        recupera os dados processados dos usuários no formato CSV

        :return: os dados no formato CSV
        """

        return self._df_user.to_csv(sep=",", index=False)

    def get_dependant_as_dict(self) -> dict:
        """
        recupera os dados processados dos dependentes no formato dict

        :return: os dados no formato dict
        """

        if self._df_dependant is None:
            return {}
        else:
            return self._df_dependant.to_dict('r')

    def get_dependant_as_rows(self) -> any:
        """
        recupera os dados processados dos dependentes no formato de vetores

        :return: os dados no formato de vetores
        """

        if self._df_dependant is None:
            return []
        else:
            return self._df_dependant.to_records(index=False)

    def get_dependant_as_csv(self) -> str:
        """
        recupera os dados processados dos dependentes no formato CSV

        :return: os dados no formato CSV
        """

        if self._df_dependant is None:
            return ""
        else:
            return self._df_dependant.to_csv(sep=",", index=False)

    def do_process(self) -> None:
        """
        Executa as etapas de leitura e migração dos dados.
        """

        self.load_data()
        self.prepare_data()
        self.process_data()
