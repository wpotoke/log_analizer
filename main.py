import argparse
import sys
import json
from abc import ABC, abstractmethod
from datetime import date
from tabulate import tabulate


class ReportGenerator(ABC):
    """Абстрактный базовый класс для генерации различных типов отчетов"""

    @abstractmethod
    def generate(self, input_data: dict[dict:str]) -> dict[str]:
        """Генерирует отчет на основе входных данных"""


class ReportReader(ABC):
    """Абстрактный базовый класс для чтения данных из различных источников"""

    @abstractmethod
    def read(self, source_path: str):
        """Читает данные из указанного источника"""


class ReportRender(ABC):
    """Абстрактный базовый класс для вывода отчетов в различных форматах"""

    @abstractmethod
    def render(self, report_data: dict):
        """Отображает отчет в указанном формате"""


class ReportFilter(ABC):
    """Абстрактный базовый класс для фильтрации данных отчетов"""

    @abstractmethod
    def filter(self, raw_data: list, filter_value: str):
        """Фильтрует данные по указанному критерию"""


class CustomArgumentParser(argparse.ArgumentParser):
    """Кастомный парсер аргументов командной строки с улучшенной обработкой ошибок"""

    def error(self, message: str):
        """Обрабатывает ошибки парсинга аргументов"""
        try:
            raise ValueError("Invalid parameters")
        except ValueError as error:
            print(error)
            print(message)
            print(self.print_help())
            sys.exit(1)


class JsonReader(ReportReader):
    """Реализация чтения данных из JSON-файлов"""

    def read(self, source_path: str) -> list:
        """Читает и парсит JSON данные из файла построчно"""
        with open(file=source_path, mode="r", encoding="utf-8") as json_file:
            parsed_data = []
            for json_line in json_file:
                parsed_data.append(json.loads(json_line))
            return parsed_data


class DateReportFilter(ReportFilter):
    """Фильтрация данных по дате"""

    def filter(self, raw_data: list, filter_value: str) -> list:
        """Фильтрует данные, оставляя только записи с указанной датой"""
        year, month, day = map(int, filter_value.split("-"))
        target_date = date(year, month, day)

        filtered_records = []
        for record in raw_data:
            if str(target_date) in record.get("@timestamp", ""):
                filtered_records.append(record)
        return filtered_records


class AverageReportGenerator(ReportGenerator):
    """Генератор отчета со средней статистикой по endpoint'ам"""

    def generate(self, input_data: list) -> dict:
        """Вычисляет среднее время ответа для каждого endpoint'а"""
        endpoint_stats = {}

        for log_entry in input_data:
            endpoint_url = log_entry.get("url")
            response_time = log_entry.get("response_time")

            if endpoint_url not in endpoint_stats:
                endpoint_stats[endpoint_url] = {"handler": endpoint_url, "total": 1, "avg_response_time": response_time}
            else:
                endpoint_stats[endpoint_url]["total"] += 1
                endpoint_stats[endpoint_url]["avg_response_time"] += response_time

        for stats in endpoint_stats.values():
            stats["avg_response_time"] = round(stats["avg_response_time"] / stats["total"], 3)

        return endpoint_stats


class TableRender(ReportRender):
    """Рендеринг отчета в виде таблицы"""

    def render(self, report_data: dict[dict]):
        """Выводит отчет в виде форматированной таблицы"""
        table_rows = [endpoint_stats.values() for endpoint_stats in report_data.values()]
        column_headers = ["Handler", "Total", "Average Response Time"]
        print(tabulate(table_rows, column_headers, tablefmt="simple"))


def merge_statistics(statistics_reports: list[dict]) -> dict:
    """Объединяет статистику из нескольких отчетов и сортирует по количеству запросов"""
    combined_stats = {}

    for report in statistics_reports:
        for endpoint, endpoint_data in report.items():
            if endpoint not in combined_stats:
                combined_stats[endpoint] = {
                    "handler": endpoint,
                    "total": endpoint_data["total"],
                    "sum_time": endpoint_data["avg_response_time"] * endpoint_data["total"],
                }
            else:
                combined_stats[endpoint]["total"] += endpoint_data["total"]
                combined_stats[endpoint]["sum_time"] += endpoint_data["avg_response_time"] * endpoint_data["total"]

    # Сортировка по убыванию количества запросов
    sorted_endpoints = sorted(combined_stats.items(), key=lambda item: item[1]["total"], reverse=True)

    # Формирование итогового отчета с порядковыми номерами
    final_report = {}
    for index, (endpoint, stats) in enumerate(sorted_endpoints):
        final_report[endpoint] = {
            "idx": index,
            "handler": endpoint,
            "total": stats["total"],
            "avg_response_time": round(stats["sum_time"] / stats["total"], 3),
        }

    return final_report


def main():
    """Основная функция обработки и анализа логов"""
    argument_parser = CustomArgumentParser(description="Анализатор логов - генерация статистики по endpoint'ам")
    argument_parser.add_argument("--file", nargs="+", required=True, help="Путь к файлу(ам) с логами")
    argument_parser.add_argument("--report", help="Создать отчет в JSON формате")
    argument_parser.add_argument("--date", default=None, help="Фильтрация по дате (формат YYYY-MM-DD)")

    parsed_args = argument_parser.parse_args()

    # Доступные компоненты системы
    readers = {"json": JsonReader}
    report_types = {"average": AverageReportGenerator}
    renderers = {"table": TableRender}
    filters = {"date_filter": DateReportFilter}

    if parsed_args.report not in report_types:
        print(f"Тип отчета {parsed_args.report} не поддерживается. " f"Доступные: {', '.join(report_types.keys())}")
        sys.exit(1)

    # Инициализация компонентов
    log_reader = readers["json"]()
    report_generator = report_types["average"]()
    report_renderer = renderers["table"]()
    data_filter = filters["date_filter"]()

    # Обработка файлов
    collected_reports = []
    for log_file in parsed_args.file:
        raw_logs = log_reader.read(log_file)
        if parsed_args.date:
            raw_logs = data_filter.filter(raw_logs, parsed_args.date)
        collected_reports.append(report_generator.generate(raw_logs))

    # Объединение и вывод результатов
    final_statistics = merge_statistics(collected_reports)
    report_renderer.render(final_statistics)


if __name__ == "__main__":
    main()
