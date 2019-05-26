import datetime

from datasetreader.CSVReader import CSVReader
from datasetreader.GSheetsReader import GSheetsReader
from datasetreader.XMLReader import XMLReader

if __name__ == '__main__':

    total_time = []
    for reader in [CSVReader('datasets/dataApr-1-2019.csv'),
                   XMLReader('datasets/dataApr-1-2019 2.xml'),
                   GSheetsReader('1N6JFMIQR71HF5u5zkWthqbgpA8WYz_0ufDGadeJnhlo')]:

        started_time = datetime.datetime.now()

        reader.do_process()
        print('========================================')
        print(' ')
        if isinstance(reader, CSVReader):
            print(f'Migração dos dados de origem: {reader.csv_filename}')
        elif isinstance(reader, XMLReader):
            print(f'Migração dos dados de origem: {reader.xml_filename}')
        elif isinstance(reader, GSheetsReader):
            print(f'Migração da planilha de origem: {reader.spreadsheet_id}')
        print(' ')
        print(reader.get_user_as_csv())
        print(' ')
        print('----------------------------------------')
        print(reader.get_dependant_as_csv())
        print(' ')

        elapsed_time = datetime.datetime.now() - started_time
        total_time.append(elapsed_time)

    print(' ')
    print('CSVReader, XMLReader, GSheetsReader')
    print([str(t) for t in total_time])
    print(' ')
    print(f'total = {sum(total_time, datetime.timedelta())}')
