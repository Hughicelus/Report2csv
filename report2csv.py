import logging
import pandas as pd
from io import BytesIO
import msoffcrypto
import xlrd
from pathlib import Path
from sqlalchemy import create_engine, Text
from PySide6.QtGui import QIcon
from PySide6.QtWidgets import QApplication, QWidget, QFileDialog, QTableWidgetItem
from PySide6.QtSql import QSqlDatabase, QSqlQuery
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

QApplication.setAttribute(Qt.AA_ShareOpenGLContexts)

logging.basicConfig(
    filename="./log/report2csv.log",
    filemode="a",
    encoding="utf-8",
    format="%(asctime)s %(message)s",
    level=logging.DEBUG,
)


DATABASE_URL = "sqlite:///db/database.db"


class Signals(QObject):
    started = Signal(int)
    completed = Signal(tuple)
    error = Signal(str)


class Worker(QRunnable):
    def __init__(self, n, file):
        super().__init__()
        self.file = file
        self.n = n
        self.signals = Signals()
        self.engine = create_engine(DATABASE_URL)
        self.mutex = QMutex()

    @Slot()
    def run(self):
        with QMutexLocker(self.mutex):
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
                category = "88卡"
                df = pd.read_excel(xls, sheet_name="88-SYNTH", header=None)
                number = df.iloc[4:6, 5].dropna().values[0]
                title = pd.read_excel(xls, sheet_name="88PRES", header=None).iloc[8, 6]
                icmd = float(df.iloc[27, 3])  # 确保数值类型
                icmc = float(df.iloc[27, 5])
                dfs = [
                    xls.parse(sheet).iloc[8:54, [15, 16, 17, 21, 22, 23, 24, 25, 26]]
                    for sheet in xls.sheet_names
                    if sheet.startswith("RES-")
                ]
                combined_df = pd.concat(dfs, ignore_index=True)
                combined_df.columns = [
                    "类型",
                    "编号",
                    "序号",
                    "上公差",
                    "下公差",
                    "零件1",
                    "零件2",
                    "零件3",
                    "零件4",
                ]
                combined_df.insert(0, "零件号", number)
                combined_df.insert(1, "零件名", title)
                combined_df.dropna(subset=["编号"], inplace=True)

            output_dir = Path("output")
            output_dir.mkdir(exist_ok=True)
            combined_df.to_csv(
                output_dir / f"{title}.csv", index=False, encoding="utf-8-sig"
            )
            with self.engine.connect() as conn:
                combined_df.to_sql(
                    name="ET0", con=conn, if_exists="append", dtype={"类型": Text}
                )
            return combined_df, number, title, icmd, icmc, category
        except Exception as e:
            logging.error(f"Error processing 88 card: {str(e)}")
            raise

    def process_32_card(self):
        category = "32卡"
        try:
            with pd.ExcelFile(self.file) as xls:
                df = pd.read_excel(xls, sheet_name="1(32j)", header=None)
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
                    df = pd.read_excel(decrypted, header=None, sheet_name="1(32j)")
                    decrypted.seek(0)
                    with pd.ExcelFile(decrypted) as xls:
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
            "序号",
            "上公差",
            "下公差",
            "零件1",
            "零件2",
            "零件3",
            "零件4",
        ]
        combined_df.insert(0, "零件号", number)
        combined_df.insert(1, "零件名", title)
        combined_df.dropna(subset=["编号"], inplace=True)

        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        combined_df.to_csv(
            output_dir / f"{title}.csv", index=False, encoding="utf-8-sig"
        )
        with self.engine.connect() as conn:
            combined_df.to_sql(
                name="ET0", con=conn, if_exists="append", dtype={"类型": Text}
            )
        return combined_df, number, title, icmd, icmc, category


class Widget(QWidget):
    def __init__(self):
        super().__init__()
        ui_file = Path(__file__).parent / "report2csv.ui"
        loader = QUiLoader()
        self.ui = loader.load(ui_file, self)
        self.setWindowTitle("报告转换器 0.0.1")
        self.setWindowIcon(QIcon("icon.png"))
        self.setup_slots()
        self.files = []

    def setup_slots(self):
        self.ui.pushButton.clicked.connect(self.get_files)
        self.ui.pushButton_2.clicked.connect(self.start_jobs)
        self.ui.pushButton_14.clicked.connect(self.clear_db)

    def start_jobs(self):
        if self.files:
            print(self.ui.comboBox.currentText())
            self.restart()
            pool = QThreadPool.globalInstance()
            for n, file in enumerate(self.files, start=1):
                self.ui.textEdit.append(
                    f'{QDateTime.currentDateTime().toString("yyyy-MM-dd hh:mm:ss")}, {file}'
                )
                worker = Worker(n, file)
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
        self.ui.tableWidget.setItem(row, 2, QTableWidgetItem(f"{icmd:.2%}"))
        self.ui.tableWidget.setItem(row, 3, QTableWidgetItem(f"{icmc:.2%}"))
        self.ui.tableWidget.setItem(row, 4, QTableWidgetItem(category))
        self.ui.tableWidget.resizeColumnsToContents()
        self.ui.tableWidget.scrollToBottom()

        self.ui.progressBar.setValue(len(self.completed_jobs))
        if len(self.completed_jobs) == len(self.files):
            self.ui.pushButton_2.setEnabled(True)
            self.ui.pushButton_2.setStyleSheet("background-color: rgb(0, 170, 0); color: white")
            self.ui.pushButton_2.setText("开始")

    def clear_db(self):
        db = QSqlDatabase.addDatabase("QSQLITE")
        db.setDatabaseName("db/database.db")
        if db.open():
            query = QSqlQuery(db)
            # query.exec("DROP TABLE IF EXISTS ET0")
            query.exec("DELETE FROM ET0")
            query.exec("DELETE FROM sqlite_sequence WHERE name='ETO'")

    def get_files(self):
        self.files, _ = QFileDialog.getOpenFileNames(
            self,
            "打开文件",
            r"E:\Project\S32\06-零件报告\MDL\外制\敏实",
            "Excel文件 (*.xls*)",
        )
        if self.files:
            self.ui.progressBar.setMaximum(len(self.files))


if __name__ == "__main__":
    app = QApplication([])
    widget = Widget()
    widget.show()
    app.exec()
