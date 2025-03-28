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

QApplication.setAttribute(Qt.AA_ShareOpenGLContexts)

logging.basicConfig(
    filename="./log/report2csv.log",
    filemode="a",
    encoding="utf-8-sig",
    format="%(asctime)s %(message)s",
    level=logging.DEBUG,
)


DATABASE_URL = "sqlite:///db/database.db"
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

                combined_df, number, title, icmd, icmc, category = result
                msg = (self.n, self.file, number, title, icmd, icmc, category)
                self.signals.completed.emit(msg)
            except Exception as e:
                logging.error(f"Error processing {self.file}: {str(e)}")

    def process_88_card(self):
        try:
            with pd.ExcelFile(self.file) as xls:
                if "88PRES" in xls.sheet_names:
                    category = "88卡"
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
                        "类型",
                        "编号",
                        "点号",
                        "上公差",
                        "下公差",
                        "零件1",
                        "零件2",
                        "零件3",
                        "零件4",
                    ]
                    combined_df.insert(0, "导入序号", self.n)
                    combined_df.insert(1, "零件号", number)
                    combined_df.insert(2, "零件名", title)
                    combined_df.insert(3, "阶段", self.stage)
                    combined_df.dropna(subset=["编号"], inplace=True)

                    output_dir = Path("output")
                    output_dir.mkdir(exist_ok=True)
                    combined_df.to_csv(
                        output_dir / f"{title}.csv", index=False, encoding="utf-8-sig"
                    )
                    with self.engine.connect() as conn:
                        combined_df.to_sql(
                            name=self.stage,
                            con=conn,
                            if_exists="append",
                        )
                    return combined_df, number, title, icmd, icmc, category

        except Exception as e:
            logging.error(f"88卡错误: {str(e)}")
            raise

    def process_32_card(self):
        category = "32卡"
        try:
            with pd.ExcelFile(self.file) as xls:
                # df = pd.read_excel(xls, sheet_name="1(32j)", header=None)
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
                    # df = pd.read_excel(decrypted, header=None, sheet_name="1(32j)")
                    # decrypted.seek(0)
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
            "类型",
            "编号",
            "点号",
            "上公差",
            "下公差",
            "零件1",
            "零件2",
            "零件3",
            "零件4",
        ]
        combined_df.insert(0, "导入序号", self.n)
        combined_df.insert(1, "零件号", number)
        combined_df.insert(2, "零件名", title)
        combined_df.insert(3, "阶段", self.stage)
        combined_df.dropna(subset=["编号"], inplace=True)

        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        combined_df.to_csv(
            output_dir / f"{title}.csv", index=False, encoding="utf-8-sig"
        )
        with self.engine.connect() as conn:
            combined_df.to_sql(name=self.stage, con=conn, if_exists="append")
        return combined_df, number, title, icmd, icmc, category


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

    def setup_slot(self):
        self.ui.pushButton.clicked.connect(self.get_files)
        self.ui.pushButton_2.clicked.connect(self.start_jobs)
        self.ui.pushButton_5.clicked.connect(self.clear_db)
        self.ui.pushButton_6.clicked.connect(self.clear_log)
        self.ui.pushButton_9.clicked.connect(self.model_table)
        self.ui.pushButton_10.clicked.connect(self.delete_db)
        self.ui.pushButton_21.clicked.connect(self.get_folder)

    def setup_confie(self):
        self.ui.comboBox.addItems()   

    def start_jobs(self):
        if self.files:
            self.stage = self.ui.comboBox.currentText()
            self.restart()
            pool = QThreadPool.globalInstance()
            for n, file in enumerate(self.files, start=1):
                self.ui.textEdit.append(
                    f'{QDateTime.currentDateTime().toString("yyyy-MM-dd hh:mm:ss")}, {file}'
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
        n, file, number, title, icmd, icmc, category = msg
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
        self.ui.tableWidget.setItem(
            row,
            6,
            QTableWidgetItem(
                QDateTime.currentDateTime().toString("yyyy-MM-dd hh:mm:ss")
            ),
        )
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
            # query.exec("DROP TABLE IF EXISTS {self.stage}")
            query.exec(f"DELETE FROM {self.stage}")
            query.exec(f"DELETE FROM sqlite_sequence WHERE name={self.stage}")

    def delete_db(self):
        file_db = Path("db/database.db")
        if file_db.exists():
            file_db.unlink()

    def clear_log(self):
        with open("log/report2csv.log", "w") as f:
            f.write("")

    def get_folder(self):
        _folder = QFileDialog.getExistingDirectory(
            self,
            '打开文件夹',
            r"E:\Project\S32\06-零件报告\MDL",

        )
        self.files= list(map(str, Path(_folder).rglob("*.xls*")))
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
        # db = QSqlDatabase.addDatabase("QSQLITE")
        # db.setDatabaseName("db/database.db")
        # db.open()
        if QSqlDatabase.contains("qt_sql_default_connection"):
            QSqlDatabase.removeDatabase("qt_sql_default_connection")
        db = QSqlDatabase.addDatabase("QSQLITE")
        db.setDatabaseName("db/database.db")
        if db.open():
            self.stage = self.ui.comboBox.currentText()
            self.total_model = QSqlQueryModel(self)
            self.total_model.setQuery(f"select * from {self.stage}")
            self.ui.tableView.setModel(self.total_model)


if __name__ == "__main__":
    app = QApplication([])
    widget = Widget()
    app.exec()
