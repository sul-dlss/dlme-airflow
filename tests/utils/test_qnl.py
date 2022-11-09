import pandas
from ast import literal_eval

from dlme_airflow.utils.qnl import *


def test_merge():
    df = pandas.read_csv("tests/data/csv/qnl.csv")
    assert len(df) == 20
    assert len(df.columns) == len(
        merge_df(df).columns
    ), "number of columns didn't change"

    df = merge_df(df)
    assert len(df) == 10
    assert [isinstance(i, list) for i in df["subject_name_namePart"]]

    assert (
        squash_lists(df.subject_topic[1])
        == "['Medicine, Arab', 'Medicine, Medieval', 'Medicine, Greek and Roman', 'الطب عند العرب', 'الطب في العصور الوسطى', 'طب، إغريقي وروماني']"
    )
    assert (
        squash_lists(df.physicalDescription_extent[1])
        == "['Material: PaperDimensions: 295 x 212 mm leaf [198 x 127 mm written]Foliation: British Museum foliation in pencil; Arabic pagination in red inkRuling: Misṭarah; 19 lines per page; vertical spacing 10 lines per 10 cm; the written area is enclosed by a frame of double lines in red inkScript: NaskhInk: Black ink, with rubricated headings and overlinings in red; text frame in red inkBinding: British Museum binding in dark brown leatherCondition: Excellent conditionMarginalia: None', 'المادة: ورقيةالأبعاد: حجم الورقة ٢٩٥ × ٢١٢ مم /[المساحة المكتوبة ١٩٨ × ١٢٧ مم]ترقيم الأوراق: ترقيم المتحف البريطاني باستخدام قلم رصاص؛ ترقيم الصفحات باللغة العربية بالحبر الأحمرالتسطير: مسطرة؛ ١٩ سطرًا في كل صفحة؛ مسافة رأسية ١٠ سطور كل ١٠ سم؛ يحيط بالمساحة المكتوبة إطار من سطرين مزدوجين بالحبر الأحمرالخط: نسخالحبر: حبر أسود مع تحمير العناوين والخطوط الأفقية أعلى النص بالحبر الأحمر؛ إطار النص بالحبر الأحمرالتجليد: تجليد المتحف البريطاني بالجلد البني الداكنالحالة: حالة ممتازةالحواشي: لا يوجد']"
    )
    df = squash_df(df)
    assert len(literal_eval(df.subject_topic[1])) == 6
    assert len(literal_eval(df.subject_name_namePart[1])) == 10
