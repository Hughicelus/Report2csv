import logging
import pandas as pd
from io import BytesIO
import msoffcrypto
import xlrd
from pathlib import Path
from sqlalchemy import create_engine, Text
from PySide6.QtGui import QIcon
from PySide6.QtWidgets import (
    QApplication,
    QWidget,
    QFileDialog,
    QTableWidgetItem,
    QHeaderView,
)
from PySide6.QtSql import QSqlDatabase, QSqlQuery, QSqlQueryModel
from PySide6.QtCore import (
    QObject,
    QDateTime,
    Signal,
    Slot,
    QRunnable,
    QThreadPool,
    Qt,
    QMutex,
    QMutexLocker,
)
from PySide6.QtUiTools import QUiLoader

import config

STAGE_LIST = config.STAGE_LIST
DATABASE_URL = config.DATABASE_URL


QApplication.setAttribute(Qt.AA_ShareOpenGLContexts)

logging.basicConfig(
    filename="./log/report2csv.log",
    filemode="a",
    encoding="utf-8-sig",
    format="%(asctime)s %(message)s",
    level=logging.DEBUG,
)


mutex = QMutex()


class Signals(QObject):
    started = Signal(int)
    completed = Signal(tuple)
    error = Signal(str)


class Worker(QRunnable):
    def __init__(self, n, file, stage):
        super().__init__()
        self.n = n
        self.file = file
        self.stage = stage
        self.signals = Signals()
        self.engine = create_engine(DATABASE_URL)

    @Slot()
    def run(self):
        with QMutexLocker(mutex):
            try:
                self.signals.started.emit(self.n)
                if "-88" in self.file:
                    result = self.process_88_card()
                elif "F32" in self.file:
                    result = self.process_32_card()
                else:
                    raise ValueError("Unsupported file type")

                time, number, title, icmd, icmc, category = result
                msg = (self.n, self.file, number, title, icmd, icmc, category, time)
                self.signals.completed.emit(msg)
            except Exception as e:
                logging.error(f"Error processing {self.file}: {str(e)}")

    def process_88_card(self):
        try:
            with pd.ExcelFile(self.file) as xls:
                if "88PRES" in xls.sheet_names:
                    category = "88卡"
                    time = QDateTime.currentDateTime().toString(
                        "yyyy-MM-dd hh:mm:ss zzz"
                    )
                    df = pd.read_excel(xls, sheet_name="88-SYNTH", header=None)

                    number = df.iloc[4:6, 5].dropna().values[0]
                    title = pd.read_excel(xls, sheet_name="88PRES", header=None).iloc[
                        8, 6
                    ]
                    icmd = float(df.iloc[27, 3])  # 确保数值类型
                    icmc = float(df.iloc[27, 5])
                    dfs = [
                        xls.parse(
                            sheet,
                            header=None,
                            usecols="P,Q,R,V,W,X,Y,Z,AA",
                            skiprows=9,
                            nrows=46,
                        )
                        for sheet in xls.sheet_names
                        if sheet.startswith("RES-")
                    ]

                    combined_df = pd.concat(dfs, ignore_index=True)
                    combined_df.columns = [
                        "category",
                        "number",
                        "code",
                        "upper_tolerance",
                        "lower_tolerance",
                        "part1",
                        "part2",
                        "part3",
                        "part4",
                    ]
                    combined_df.insert(0, "no", self.n)
                    combined_df.insert(1, "part", number)
                    combined_df.insert(2, "name", title)
                    combined_df.insert(3, "stage", self.stage)
                    combined_df.dropna(subset=["code"], inplace=True)

                    output_dir = Path("output")
                    output_dir.mkdir(exist_ok=True)
                    combined_df.to_csv(
                        output_dir / f"{title}.csv", index=False, encoding="utf-8-sig"
                    )
                    total_df = pd.DataFrame(
                        [
                            [
                                number,
                                title,
                                self.stage,
                                icmd,
                                icmc,
                                category,
                                time,
                                self.file,
                            ]
                        ],
                        columns=[
                            "number",
                            "title",
                            "category",
                            "icmd",
                            "icmc",
                            "stage",
                            "date",
                            "file",
                        ],
                    )
                    with self.engine.connect() as conn:
                        combined_df.to_sql(
                            name=self.stage,
                            con=conn,
                            if_exists="append",
                        )
                        total_df.to_sql(
                            name="Total", con=conn, if_exists="append", index=False
                        )
                    return time, number, title, icmd, icmc, category

        except Exception as e:
            logging.error(f"88卡错误: {str(e)}")
            raise

    def process_32_card(self):
        category = "32卡"
        time = QDateTime.currentDateTime().toString("yyyy-MM-dd hh:mm:ss zzz")
        try:
            with pd.ExcelFile(self.file) as xls:
                df = xls.parse("1(32j)", header=None)
                dfs = [
                    xls.parse(
                        sheet,
                        header=None,
                        usecols="X,AC,AE,AG,AH,AK,AL,AM,AN",
                        skiprows=9,
                        nrows=64,
                    )
                    for sheet in xls.sheet_names
                    if sheet.endswith("(32i)")
                ]
        except xlrd.biffh.XLRDError:
            decrypted = BytesIO()
            with open(self.file, "rb") as f:
                _file = msoffcrypto.OfficeFile(f)
                if _file.is_encrypted():
                    _file.load_key(password="VelvetSweatshop")
                    _file.decrypt(decrypted)
                    decrypted.seek(0)
                    with pd.ExcelFile(decrypted) as xls:
                        df = xls.parse("1(32j)", header=None)
                        dfs = [
                            xls.parse(
                                sheet,
                                header=None,
                                usecols="X,AC,AE,AG,AH,AK,AL,AM,AN",
                                skiprows=9,
                                nrows=64,
                            )
                            for sheet in xls.sheet_names
                            if sheet.endswith("(32i)")
                        ]
        number = df.iloc[2:5, 3].dropna().values[0].replace(" ", "")
        title = df.iloc[0, 2]
        icmd = float(df.iloc[22, 8])  # 转换为数值类型
        icmc = float(df.iloc[22, 6])
        combined_df = pd.concat(dfs, ignore_index=True)
        combined_df.columns = [
            "category",
            "number",
            "code",
            "upper_tolerance",
            "lower_tolerance",
            "part1",
            "part2",
            "part3",
            "part4",
        ]
        combined_df.insert(0, "no", self.n)
        combined_df.insert(1, "part", number)
        combined_df.insert(2, "name", title)
        combined_df.insert(3, "stage", self.stage)
        combined_df.dropna(subset=["code"], inplace=True)

        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        combined_df.to_csv(
            output_dir / f"{title}.csv", index=False, encoding="utf-8-sig"
        )
        total_df = pd.DataFrame(
            [
                [
                    number,
                    title,
                    self.stage,
                    icmd,
                    icmc,
                    category,
                    time,
                    self.file,
                ]
            ],
            columns=[
                "number",
                "title",
                "category",
                "icmd",
                "icmc",
                "stage",
                "date",
                "file",
            ],
        )
        with self.engine.connect() as conn:
            combined_df.to_sql(name=self.stage, con=conn, if_exists="append")
            total_df.to_sql(name="Total", con=conn, if_exists="append", index=False)
        return time, number, title, icmd, icmc, category


class Widget(QWidget):
    def __init__(self):
        super().__init__()
        self.setup_ui()
        self.setup_dir()
        self.setup_slot()

    def setup_ui(self):
        self.ui = QUiLoader().load("report2csv.ui")
        self.ui.show()

    def setup_dir(self):
        db_dir = Path("db")
        db_dir.mkdir(exist_ok=True)
        log_dir = Path("log")
        log_dir.mkdir(exist_ok=True)
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        csv_dir = Path("csv")
        csv_dir.mkdir(exist_ok=True)
        self.files = []
        self.stage = self.ui.comboBox.currentText()

    def setup_slot(self):
        self.ui.pushButton.clicked.connect(self.get_files)
        self.ui.pushButton_2.clicked.connect(self.start_jobs)
        self.ui.pushButton_5.clicked.connect(self.clear_db)
        self.ui.pushButton_6.clicked.connect(self.clear_log)
        self.ui.pushButton_9.clicked.connect(self.model_table)
        self.ui.pushButton_10.clicked.connect(self.delete_db)
        self.ui.pushButton_21.clicked.connect(self.get_folder)
        self.ui.pushButton_11.clicked.connect(self.clear_total)
        self.ui.pushButton_12.clicked.connect(self.model_total)

    def setup_config(self):
        self.ui.comboBox.addItems()

    def start_jobs(self):
        if self.files:
            self.stage = self.ui.comboBox.currentText()
            self.restart()
            pool = QThreadPool.globalInstance()
            for n, file in enumerate(self.files, start=1):
                self.ui.textEdit.append(
                    f'{QDateTime.currentDateTime().toString("yyyy-MM-dd hh:mm:ss zzz")}, {file}'
                )
                worker = Worker(n, file, self.stage)
                worker.signals.completed.connect(self.complete)
                worker.signals.started.connect(self.start)
                pool.start(worker)

    def restart(self):
        self.ui.progressBar.setValue(0)
        self.completed_jobs = []
        self.ui.pushButton_2.setEnabled(False)
        self.ui.pushButton_2.setText("运行中")
        self.ui.pushButton_2.setStyleSheet("background-color: red;color: white")

        self.ui.tableWidget.setRowCount(0)  # 清空表格

    def start(self, n):
        self.ui.listWidget.addItem(f"任务 #{n} 已启动...")
        self.ui.lineEdit.setText(f"{n}/{len(self.files)}: {Path(self.files[n-1]).name}")

    def complete(self, msg):
        n, file, number, title, icmd, icmc, category, time = msg
        self.ui.listWidget.addItem(f"任务 #{n} 已完成")
        self.completed_jobs.append(n)

        row = self.ui.tableWidget.rowCount()
        self.ui.tableWidget.insertRow(row)
        self.ui.tableWidget.setItem(row, 0, QTableWidgetItem(str(number)))
        self.ui.tableWidget.setItem(row, 1, QTableWidgetItem(title))
        self.ui.tableWidget.setItem(row, 2, QTableWidgetItem(self.stage))
        self.ui.tableWidget.setItem(row, 3, QTableWidgetItem(f"{icmd:.2%}"))
        self.ui.tableWidget.setItem(row, 4, QTableWidgetItem(f"{icmc:.2%}"))
        self.ui.tableWidget.setItem(row, 5, QTableWidgetItem(category))
        self.ui.tableWidget.setItem(row, 6, QTableWidgetItem(time))
        self.ui.tableWidget.setItem(row, 7, QTableWidgetItem(file))
        self.ui.tableWidget.resizeColumnsToContents()
        self.ui.tableWidget.scrollToBottom()

        self.ui.progressBar.setValue(len(self.completed_jobs))
        if len(self.completed_jobs) == len(self.files):
            self.ui.pushButton_2.setEnabled(True)
            self.ui.pushButton_2.setStyleSheet(
                "background-color: rgb(0, 170, 0); color: white"
            )
            self.ui.pushButton_2.setText("开始")

    def clear_db(self):
        db = QSqlDatabase.addDatabase("QSQLITE")
        db.setDatabaseName("db/database.db")
        if db.open():
            query = QSqlQuery(db)
            query.exec(f"DROP TABLE IF EXISTS {self.stage}")
            # query.exec(f"DELETE FROM {self.stage}")
            # query.exec(f"DELETE FROM sqlite_sequence WHERE name={self.stage}")
        self.ui.lineEdit_3.setText(f"{self.stage}表已清空")

    def clear_total(self):
        db = QSqlDatabase.addDatabase("QSQLITE")
        db.setDatabaseName("db/database.db")
        if db.open():
            query = QSqlQuery(db)
            query.exec("DROP TABLE IF EXISTS 'Total'")
        self.ui.lineEdit_3.setText("总表已清空")

    def delete_db(self):
        file_db = Path("db/database.db")
        if file_db.exists():
            file_db.unlink()
        self.ui.lineEdit_3.setText("数据库已删除")
        # self.ui.lineEdit_3.setStyleSheet("color: red")

    def clear_log(self):
        with open("log/report2csv.log", "w") as f:
            f.write("")
        self.ui.lineEdit_3.setText("日志已清空")

    def get_folder(self):
        _folder = QFileDialog.getExistingDirectory(
            self,
            "打开文件夹",
            r"E:\Project\S32\06-零件报告\MDL",
        )
        self.files = list(map(str, Path(_folder).rglob("*.xls*")))
        if self.files:
            self.ui.progressBar.setMaximum(len(self.files))

    def get_files(self):
        self.files, _ = QFileDialog.getOpenFileNames(
            self,
            "打开文件",
            r"E:\Project\S32\06-零件报告\MDL\外制\东实",
            "Excel文件 (*.xls*)",
        )
        if self.files:
            self.ui.progressBar.setMaximum(len(self.files))

    def model_table(self):
        if QSqlDatabase.contains("qt_sql_default_connection"):
            QSqlDatabase.removeDatabase("qt_sql_default_connection")
        db = QSqlDatabase.addDatabase("QSQLITE")
        db.setDatabaseName("db/database.db")
        if db.open():
            self.stage = self.ui.comboBox.currentText()
            self.table_model = QSqlQueryModel(self)
            self.table_model.setQuery(f"select * from {self.stage}")
            self.ui.tableView.setModel(self.table_model)

    def model_total(self):
        if QSqlDatabase.contains("qt_sql_default_connection"):
            QSqlDatabase.removeDatabase("qt_sql_default_connection")
        db = QSqlDatabase.addDatabase("QSQLITE")
        db.setDatabaseName("db/database.db")
        if db.open():
            self.stage = self.ui.comboBox.currentText()
            self.total_model = QSqlQueryModel(self)
            self.total_model.setQuery(f"select * from 'total'")
            self.ui.tableView_2.setModel(self.total_model)
            self.ui.tableView_2.resizeColumnsToContents()


if __name__ == "__main__":
    app = QApplication([])
    widget = Widget()
    app.exec()
