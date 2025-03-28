from PySide6.QtUiTools import QUiLoader
from PySide6.QtWidgets import QWidget, QApplication


class Widget(QWidget):
    def __init__(self):
        super().__init__()
        self.ui = QUiLoader().load("report2csv.ui")
        print([i for i in self.ui.__dict__])


if __name__ == "__main__":
    app = QApplication([])
    windows = Widget()
    windows.ui.show()
    app.exec()
