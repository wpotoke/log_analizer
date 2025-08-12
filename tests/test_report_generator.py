import sys
from unittest.mock import patch, mock_open
from datetime import date
import pytest

# pylint: disable=redefined-outer-name,redefined-builtin,too-many-positional-arguments

# Импортируем все классы из вашего модуля
from report_generator import (
    JsonReader,
    DateReportFilter,
    AverageReportGenerator,
    TableRender,
    ReportEngine,
    CustomArgumentParser,
)


# Фикстуры для тестовых данных
@pytest.fixture
def sample_log_data():
    return [
        '{"@timestamp": "2023-01-01T12:00:00", "url": "/api/users", "response_time": 100}\n',
        '{"@timestamp": "2023-01-01T12:01:00", "url": "/api/products", "response_time": 200}\n',
        '{"@timestamp": "2023-01-02T12:00:00", "url": "/api/users", "response_time": 150}\n',
    ]


@pytest.fixture
def parsed_log_data():
    return [
        {"@timestamp": "2023-01-01T12:00:00", "url": "/api/users", "response_time": 100},
        {"@timestamp": "2023-01-01T12:01:00", "url": "/api/products", "response_time": 200},
        {"@timestamp": "2023-01-02T12:00:00", "url": "/api/users", "response_time": 150},
    ]


@pytest.fixture
def filtered_log_data():
    return [
        {"@timestamp": "2023-01-01T12:00:00", "url": "/api/users", "response_time": 100},
        {"@timestamp": "2023-01-01T12:01:00", "url": "/api/products", "response_time": 200},
    ]


# Валидатор даты для аргументов
def validate_date(date_str):
    try:
        year, month, day = map(int, date_str.split("-"))
        date(year, month, day)
        return date_str
    except ValueError as e:
        raise ValueError("Invalid date format") from e


# Тесты для JsonReader
class TestJsonReader:
    @pytest.mark.parametrize(
        "file_content,expected",
        [
            (['{"key": "value"}\n', '{"another": "data"}\n'], [{"key": "value"}, {"another": "data"}]),
            (['{"key": "value"}\n', "invalid json\n"], [{"key": "value"}]),
            ([], []),
        ],
    )
    def test_read(self, file_content, expected):
        m = mock_open(read_data="".join(file_content))
        with patch("builtins.open", m):
            reader = JsonReader()
            result = reader.read("dummy_path")
            assert result == expected

    def test_read_file_not_found(self):
        reader = JsonReader()
        with pytest.raises(FileNotFoundError):
            reader.read("nonexistent_file.json")


# Тесты для DateReportFilter
class TestDateReportFilter:
    @pytest.mark.parametrize("date_str,expected_count", [("2023-01-01", 2), ("2023-01-02", 1), ("2023-01-03", 0)])
    def test_filter(self, parsed_log_data, date_str, expected_count):
        filter = DateReportFilter()
        result = filter.filter(parsed_log_data, date_str)
        assert len(result) == expected_count

    @pytest.mark.parametrize("invalid_date", ["invalid-date", "2023-13-01", "2023-01-32"])
    def test_filter_invalid_date(self, parsed_log_data, invalid_date):
        filter = DateReportFilter()
        with pytest.raises(ValueError):
            filter.filter(parsed_log_data, invalid_date)


# Тесты для AverageReportGenerator
class TestAverageReportGenerator:
    @pytest.mark.parametrize(
        "input_data,expected",
        [
            (
                [{"url": "/api/test", "response_time": 100}],
                {"/api/test": {"handler": "/api/test", "total": 1, "avg_response_time": 100}},
            ),
            (
                [{"url": "/api/test", "response_time": 100}, {"url": "/api/test", "response_time": 200}],
                {"/api/test": {"handler": "/api/test", "total": 2, "avg_response_time": 150}},
            ),
            (
                [{"url": "/api/test1", "response_time": 100}, {"url": "/api/test2", "response_time": 200}],
                {
                    "/api/test1": {"handler": "/api/test1", "total": 1, "avg_response_time": 100},
                    "/api/test2": {"handler": "/api/test2", "total": 1, "avg_response_time": 200},
                },
            ),
        ],
    )
    def test_generate(self, input_data, expected):
        generator = AverageReportGenerator()
        result = generator.generate(input_data)
        assert result == expected

    def test_generate_empty_input(self):
        generator = AverageReportGenerator()
        with pytest.raises(ValueError):
            generator.generate([])


# Тесты для TableRender
class TestTableRender:
    @pytest.mark.parametrize(
        "report_data",
        [
            {
                "/api/users": {"idx": 0, "handler": "/api/users", "total": 2, "avg_response_time": 125.0},
                "/api/products": {"idx": 1, "handler": "/api/products", "total": 1, "avg_response_time": 200.0},
            },
            {"/api/test": {"idx": 0, "handler": "/api/test", "total": 1, "avg_response_time": 100.0}},
        ],
    )
    def test_render(self, report_data, capsys):
        render = TableRender()
        render.render(report_data)
        captured = capsys.readouterr()

        for endpoint in report_data:
            stats = report_data[endpoint]
            assert endpoint in captured.out
            assert str(stats["total"]) in captured.out
            assert str(int(stats["avg_response_time"])) in captured.out

    def test_render_empty_data(self):
        render = TableRender()
        with pytest.raises(ValueError):
            render.render({})


# Тесты для ReportEngine
class TestReportEngine:
    @pytest.mark.parametrize(
        "files,date_filter,expected_stats",
        [
            (
                ["file1.log"],
                None,
                {
                    "/api/users": {"total": 2, "avg_response_time": 125.0},
                    "/api/products": {"total": 1, "avg_response_time": 200.0},
                },
            ),
            (
                ["file1.log"],
                "2023-01-01",
                {
                    "/api/users": {"total": 1, "avg_response_time": 100.0},
                    "/api/products": {"total": 1, "avg_response_time": 200.0},
                },
            ),
        ],
    )
    def test_run_success(self, files, date_filter, expected_stats, parsed_log_data, filtered_log_data, capsys):
        mock_reader = JsonReader()
        mock_reader.read = lambda x: parsed_log_data if x == "file1.log" else []

        mock_filter = DateReportFilter()
        mock_filter.filter = lambda data, date: filtered_log_data if date == "2023-01-01" else data

        engine = ReportEngine(
            reader=mock_reader, generator=AverageReportGenerator(), render=TableRender(), report_filter=mock_filter
        )

        engine.run(files, date_filter)
        captured = capsys.readouterr()

        for endpoint, stats in expected_stats.items():
            assert endpoint in captured.out
            assert str(stats["total"]) in captured.out
            assert str(int(stats["avg_response_time"])) in captured.out

    @pytest.mark.parametrize(
        "files,date_filter,expected_error",
        [
            ([], None, "No input files provided"),
            (["file1.log"], "invalid-date", "Invalid date format"),
            (["file1.log"], "2023-01-01", "Date filter provided but no filter implementation configured"),
        ],
    )
    def test_run_failures(self, files, date_filter, expected_error):
        engine = ReportEngine(
            reader=JsonReader(),
            generator=AverageReportGenerator(),
            render=TableRender(),
            report_filter=None if "no filter" in expected_error else DateReportFilter(),
        )

        with pytest.raises(ValueError) as excinfo:
            engine.run(files, date_filter)
        assert expected_error in str(excinfo.value)


# Тесты для CustomArgumentParser
class TestCustomArgumentParser:
    @pytest.mark.parametrize(
        "args,expected",
        [
            (
                ["--file", "file1.log", "--report", "average"],
                {"file": ["file1.log"], "report": "average", "date": None},
            ),
            (
                ["--file", "file1.log", "file2.log", "--report", "average"],
                {"file": ["file1.log", "file2.log"], "report": "average", "date": None},
            ),
            (
                ["--file", "file1.log", "--report", "average", "--date", "2023-01-01"],
                {"file": ["file1.log"], "report": "average", "date": "2023-01-01"},
            ),
        ],
    )
    def test_parse_args_success(self, args, expected):
        parser = CustomArgumentParser()
        parser.add_argument("--file", nargs="+", required=True)
        parser.add_argument("--report")
        parser.add_argument("--date", type=validate_date)

        result = parser.parse_args(args)
        assert result.file == expected["file"]
        assert result.report == expected["report"]
        assert result.date == expected["date"]

    @pytest.mark.parametrize(
        "args,expected_error",
        [
            (["--unknown", "value"], "Неизвестные аргументы"),
            (["--report", "average"], "the following arguments are required: --file"),
            (["--file", "file.log", "--date", "invalid"], "Invalid date format"),
        ],
    )
    def test_parse_args_failures(self, args, expected_error, capsys):
        parser = CustomArgumentParser()
        parser.add_argument("--file", nargs="+", required=True)
        parser.add_argument("--report")
        parser.add_argument("--date", type=validate_date)

        with patch.object(sys, "exit"):
            try:
                parser.parse_args(args)
            except SystemExit:
                captured = capsys.readouterr()
                assert expected_error in captured.out or expected_error in captured.err
