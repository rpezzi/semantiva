import pytest
import numpy as np

from semantiva.specializations.image.image_data_types import (
    ImageDataType,
    FloatAnnotatedImageStack,
)


##############################
# Helpers for Test Data
##############################


def make_2d_array(height: int, width: int, fill_value: float = 0.0) -> np.ndarray:
    """
    Creates a 2D numpy array of shape (height, width) filled with fill_value.
    """
    return np.full((height, width), fill_value, dtype=np.float32)


def make_image_datatype(
    height: int, width: int, fill_value: float = 0.0
) -> ImageDataType:
    """
    Creates an ImageDataType instance with a 2D array of shape (height, width).
    """
    arr = make_2d_array(height, width, fill_value)
    return ImageDataType(arr)


##############################
# Test Cases for FloatAnnotatedImageStack
##############################


def test_empty_initialization():
    """
    Test creating an empty FloatAnnotatedImageStack. The data should have shape (0,0,0),
    and annotation should be empty.
    """
    image_stack = FloatAnnotatedImageStack()
    assert image_stack.data.shape == (0, 0, 0)
    assert len(image_stack.annotations) == 0
    assert len(image_stack) == 0


def test_initialization_with_data_and_parameters():
    """
    Test creating a FloatAnnotatedImageStack with initial 3D data and matching annotation parameters.
    """
    # Create a stack of shape (3, 5, 5)
    data = np.zeros((3, 5, 5), dtype=np.float32)
    annotations = [10.0, 20.0, 30.0]

    image_stack = FloatAnnotatedImageStack(data=data, annotations=annotations)
    assert image_stack.data.shape == (3, 5, 5)
    assert len(image_stack.annotations) == 3
    assert len(image_stack) == 3


def test_initialization_mismatched_lengths():
    """
    Test that providing data of shape (3, H, W) but annotations of length 2
    raises a ValueError.
    """
    data = np.zeros((3, 5, 5), dtype=np.float32)
    annotations = [10.0, 20.0]  # Only 2 parameters

    with pytest.raises(ValueError) as exc_info:
        _ = FloatAnnotatedImageStack(data=data, annotations=annotations)
    assert "Inconsistent lengths: 3 images vs. 2 parameters" in str(exc_info.value)


def test_append_one_image():
    """
    Test appending one image and one annotation parameter to an initially empty stack.
    """
    image_stack = FloatAnnotatedImageStack()
    img = make_image_datatype(4, 4, fill_value=5.0)
    image_stack.append_with_annotation(item=img, annotation=1.5)

    assert len(image_stack) == 1
    assert image_stack.data.shape == (1, 4, 4)
    assert len(image_stack.annotations) == 1
    assert image_stack.annotations[0] == 1.5


def test_append_multiple_images():
    """
    Test appending multiple images with corresponding annotations,
    ensuring the shape and lengths stay consistent.
    """
    image_stack = FloatAnnotatedImageStack()

    img1 = make_image_datatype(4, 4, fill_value=1.0)
    img2 = make_image_datatype(4, 4, fill_value=2.0)
    img3 = make_image_datatype(4, 4, fill_value=3.0)

    image_stack.append_with_annotation(item=img1, annotation=10.0)
    image_stack.append_with_annotation(item=img2, annotation=20.0)
    image_stack.append_with_annotation(item=img3, annotation=30.0)

    assert len(image_stack) == 3
    assert image_stack.data.shape == (3, 4, 4)
    assert image_stack.annotations == [10.0, 20.0, 30.0]


def test_append_dimension_mismatch():
    """
    Test that appending an image with incompatible dimensions raises a ValueError.
    """
    image_stack = FloatAnnotatedImageStack()
    # Initialize with one image of shape (4,4)
    image_stack.append_with_annotation(
        make_image_datatype(4, 4, fill_value=1.0), annotation=10.0
    )

    # Attempt to append an image of shape (3,3)
    img_mismatch = make_image_datatype(3, 3, fill_value=2.0)
    with pytest.raises(ValueError) as exc_info:
        image_stack.append_with_annotation(item=img_mismatch, annotation=20.0)

    assert "do not match existing stack" in str(exc_info.value)


def test_iterating_stack():
    """
    Test the __iter__ method to ensure we can iterate through ImageDataType slices.
    """
    data = np.zeros((2, 3, 3), dtype=np.float32)  # 2 images, each 3x3
    annotations = [100.0, 200.0]
    image_stack = FloatAnnotatedImageStack(data=data, annotations=annotations)

    images = list(image_stack)  # Should yield 2 ImageDataType objects
    assert len(images) == 2
    for i, img in enumerate(images):
        assert isinstance(img, ImageDataType)
        assert img.data.shape == (3, 3)


def test_items_method():
    """
    Test the items() method which yields (annotations, ImageDataType) pairs.
    """
    data = np.zeros((3, 2, 2), dtype=np.float32)
    annotations = [10.0, 20.0, 30.0]
    image_stack = FloatAnnotatedImageStack(data=data, annotations=annotations)

    all_items = list(image_stack.items())
    assert len(all_items) == 3

    for idx, (param, img) in enumerate(all_items):
        assert param == annotations[idx]
        assert isinstance(img, ImageDataType)
        assert img.data.shape == (2, 2)


def test_items_mismatch_error():
    """
    Test that items() raises a ValueError if the lengths of images vs. annotation don't match.
    """
    data = np.zeros((2, 4, 4), dtype=np.float32)
    # Provide 3 annotations for 2 images
    annotations = [1.0, 2.0, 3.0]

    with pytest.raises(ValueError) as exc_info:
        stack = FloatAnnotatedImageStack(data=data, annotations=annotations)
        _ = list(stack.items())
    assert "Inconsistent lengths: 2 images vs. 3 parameters." in str(exc_info.value)
