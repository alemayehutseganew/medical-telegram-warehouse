from src.yolo_detect import derive_category


def test_category_promotional() -> None:
    assert derive_category(["person", "bottle"]) == "promotional"


def test_category_product_display() -> None:
    assert derive_category(["bottle"]) == "product_display"


def test_category_lifestyle() -> None:
    assert derive_category(["person"]) == "lifestyle"


def test_category_other() -> None:
    assert derive_category([]) == "other"
