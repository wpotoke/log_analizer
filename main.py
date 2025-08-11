import argparse
import sys
import json
from abc import ABC, abstractmethod
from datetime import date
from tabulate import tabulate


class ReportGenerator(ABC):
    @abstractmethod
    def generate(self, input_data: dict[dict:str]) -> dict[str]:
        ...


class ReportReader(ABC):
    @abstractmethod
    def read(self, source_path: str):
        ...


class ReportRender(ABC):
    @abstractmethod
    def render(self, report_data: dict):
        ...


class ReportFilter(ABC):
    @abstractmethod
    def filter(self, raw_data: list, filter_value: str):
        ...


class CustomArgumentParser(argparse.ArgumentParser):
    def error(self, message: str):
        try:
            raise ValueError("Invalid parameters")
        except ValueError as error:
            print(error)
            print(message)
            print(self.print_help())
            sys.exit(1)


class JsonReader(ReportReader):
    def read(self, source_path: str) -> list:
        with open(file=source_path, mode="r", encoding="utf-8") as json_file:
            parsed_data = []

            for json_line in json_file:
                parsed_data.append(json.loads(json_line))
            return parsed_data


class DateReportFilter(ReportFilter):
    def filter(self, raw_data: list, filter_value: str) -> list:
        year, month, day = map(int, filter_value.split("-"))
        target_date = date(year, month, day)

        filtered_records = []
        for record in raw_data:
            if str(target_date) in record.get("@timestamp", ""):
                filtered_records.append(record)
        return filtered_records


class AverageReportGenerator(ReportGenerator):
    def generate(self, input_data: list) -> dict:
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
    def render(self, report_data: dict[dict]):
        table_rows = [endpoint_stats.values() for endpoint_stats in report_data.values()]
        column_headers = ["Handler", "Total", "Average Response Time"]
        print(tabulate(table_rows, column_headers, tablefmt="simple"))


def merge_statistics(statistics_reports: list[dict]) -> dict:
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

    sorted_endpoints = sorted(combined_stats.items(), key=lambda item: item[1]["total"], reverse=True)
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
    argument_parser = CustomArgumentParser(description="Анализатор логов - генерация статистики по endpoint'ам")
    argument_parser.add_argument("--file", nargs="+", required=True, help="Путь к файлу(ам) с логами")
    argument_parser.add_argument("--report", help="Создать отчет в JSON формате")
    argument_parser.add_argument("--date", default=None, help="Фильтрация по дате (формат YYYY-MM-DD)")

    parsed_args = argument_parser.parse_args()
    readers = {"json": JsonReader}
    report_types = {"average": AverageReportGenerator}
    renderers = {"table": TableRender}
    filters = {"date_filter": DateReportFilter}

    if parsed_args.report not in report_types:
        print(f"Тип отчета {parsed_args.report} не поддерживается. " f"Доступные: {', '.join(report_types.keys())}")
        sys.exit(1)

    log_reader = readers["json"]()
    report_generator = report_types["average"]()
    report_renderer = renderers["table"]()
    data_filter = filters["date_filter"]()
    collected_reports = []
    for log_file in parsed_args.file:
        raw_logs = log_reader.read(log_file)
        if parsed_args.date:
            raw_logs = data_filter.filter(raw_logs, parsed_args.date)
        collected_reports.append(report_generator.generate(raw_logs))
    final_statistics = merge_statistics(collected_reports)
    report_renderer.render(final_statistics)


if __name__ == "__main__":
    main()
