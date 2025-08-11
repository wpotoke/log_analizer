import argparse
import sys
import json
from abc import ABC, abstractmethod
from tabulate import tabulate


class ReportGenerator(ABC):
    @abstractmethod
    def generate(self, read_data: dict[dict:str]) -> dict[str]:
        ...


class ReportReader(ABC):
    @abstractmethod
    def read(self, file_path):
        ...


class ReportRender(ABC):
    @abstractmethod
    def render(self, data):
        ...


class CustomArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        try:
            raise ValueError("Error paremeters")
        except ValueError as e:
            print(e)
            print(message)
            print(self.print_help())
            sys.exit(1)


class JsonReader(ReportReader):
    def read(self, file_path):
        with open(file=file_path, mode="r", encoding="utf-8") as file:
            res = []
            for line in file:
                res.append(json.loads(line))
            return res


class AverageReportGenerator(ReportGenerator):
    def generate(self, read_data):
        idx = 0
        generate_data = {}
        for i in read_data:
            url = i.get("url")
            response_time = i.get("response_time")

            if url not in generate_data:
                generate_data[url] = {"idx": idx, "handler": url, "total": 1, "avg_response_time": response_time}
                idx += 1
            else:
                generate_data[url]["total"] += 1
                generate_data[url]["avg_response_time"] += response_time

        for v in generate_data.values():
            v["avg_response_time"] = round(v["avg_response_time"] / v["total"], 3)

        return generate_data


class TableRender(ReportRender):
    def render(self, data: dict[dict]):
        table = [data[i].values() for i in data]
        headers = ["handler", "total", "avg_response_time"]
        print(tabulate(table, headers, tablefmt="simple"))


def main():
    parser = CustomArgumentParser()

    parser.add_argument("--file", nargs="+", type=str, required=True, help="Path to CSV file")
    parser.add_argument("--report", type=str, default=None, help="create report")

    args = parser.parse_args()

    aviable_readers = {"json": JsonReader}
    aviable_reports = {
        "average": AverageReportGenerator,
    }
    aviable_renders = {
        "table": TableRender,
    }

    if args.report not in aviable_reports:
        print(f"Отчёт {args.report} не поддерживается, доступны {', '.join(aviable_reports.keys())}")
        sys.exit(1)

    files = args.file
    reader = aviable_readers["json"]()
    generator = aviable_reports["average"]()
    render = aviable_renders["table"]()

    for file in files:
        read_data = reader.read(file)
        generate_data = generator.generate(read_data)
        render.render(generate_data)


if __name__ == "__main__":
    main()
