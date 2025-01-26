from typing import Type, TypeVar, Generic, Iterator, get_args, Optional, List, Tuple
from abc import ABC, abstractmethod

T = TypeVar("T")


class BaseDataType(ABC, Generic[T]):
    """
    Abstract generic base class for all data types in the semantic framework.

    This class provides a foundation for creating and managing various data types,
    ensuring consistency and extensibility across the framework.

    Attributes:
        _data (T): The underlying data encapsulated by the data type.
    """

    _data: T

    def __init__(self, data: T):
        """
        Initialize the BaseDataType with the provided data.

        Args:
            data (T): The data to be encapsulated by this data type.
        """
        self.validate(data)
        self._data = data

    @property
    def data(self) -> T:
        return self._data

    @data.setter
    def data(self, data: T):
        self._data = data

    @abstractmethod
    def validate(self, data: T) -> bool:
        """
        Abstract method to validate the encapsulated data.

        This method must be implemented by subclasses to define specific
        validation rules for the data type.

        Returns:
            bool: True if the data is valid, False otherwise.
        """


U = TypeVar("U", bound=BaseDataType)
X = TypeVar("X")  # Data annotation parameter type (e.g., float, tuple, etc.)


class AnnotatedDataType(BaseDataType[Tuple[U, X]]):
    """
    A wrapper class that encapsulates a data element and its associated annotation parameter.

    This allows collections to handle data and metadata together as a single entity.
    """

    _annotation: X | None

    def __init__(self, data: U, annotation: Optional[X] = None):
        """
        Initializes an AnnotatedDataType.

        Args:
            data (U): The main data element.
            annotation (X): The associated annotation parameter.
        """
        self._data = data.data
        self._annotation = annotation

    @property
    def annotation(self) -> X | None:
        """Returns the associated annotation parameter."""
        return self._annotation

    @annotation.setter
    def annotation(self, annotation: X):
        """
        Updates the annotation parameter while preserving the data.

        Args:
            value (X): The new annotation parameter.
        """
        self._annotation = annotation


E = TypeVar("E", bound=BaseDataType)
S = TypeVar("S")  # The preferred storage format for the collection


class DataCollectionType(BaseDataType[S], Generic[E, S]):
    """
    Abstract base class for collection-based data types.

    This class extends BaseDataType to handle data that comprises multiple
    elements and provides a foundation for collection-specific operations.
    """

    def __init__(self, data: Optional[S] = None):
        """
        Initializes a DataCollectionType.

        Args:
            data (Optional[S]): The initial collection data to initialize the object.
                Defaults to an empty collection via _initialize_empty().
        """
        if data is None:
            data = self._initialize_empty()
        super().__init__(data)

    @classmethod
    @abstractmethod
    def _initialize_empty(cls) -> S:
        """
        Defines how an empty DataCollectionType should be initialized.

        This method must be implemented by subclasses to return an empty
        instance of the appropriate collection storage format.
        """
        pass

    @classmethod
    def collection_base_type(cls) -> Type[E]:
        """
        Returns the base type of elements in the collection.

        This method provides the expected data type for elements in the collection
        based on the class definition.

        Returns:
            Type[E]: The expected type of elements in the collection.
        """
        # Attempt get_args(...) first to retrieve type arguments for classes that are
        # fully parameterized at runtime. This covers most modern Python generics.        args = get_args(cls)
        args = get_args(cls)
        if args:
            return args[0]  # First argument should be `E`

        # If get_args(...) yields no results, fallback to scanning __orig_bases__.
        # In certain mypy or older Python generics scenarios, type parameters are
        # registered there rather than in get_args(...).
        for base in getattr(cls, "__orig_bases__", []):
            base_args = get_args(base)
            if base_args:
                return base_args[0]  # First argument should be `E`

        raise TypeError(f"{cls} is not a generic class with defined type arguments.")

    @abstractmethod
    def __iter__(self) -> Iterator[E]:
        """
        Returns an iterator over elements of type E within the collection.

        Returns:
            Iterator[E]: An iterator yielding elements of type E.
        """
        pass

    @abstractmethod
    def append(self, item: E) -> None:
        """
        Appends an element of type E to the data collection.

        Subclasses should implement how elements are added to the underlying
        storage format while ensuring consistency.

        Args:
            item (E): The element to append to the collection.

        Raises:
            TypeError: If the item type does not match the expected element type.
        """
        pass

    @abstractmethod
    def __len__(self) -> int:
        """
        Returns the number of elements in the data collection.

        Subclasses must implement this method to return the number of stored elements.

        Returns:
            int: The number of elements in the collection.
        """
        pass


Y = TypeVar("Y")  # Data annotation type (e.g., float, tuple, etc.)
V = TypeVar("V", bound=BaseDataType)


class AnnotatedDataCollection(DataCollectionType[V, S], Generic[V, S, Y]):
    """
    A generic data collection that stores annotation parameters alongside its elements.

    Extends DataCollectionType to include additional metadata that associates
    annotation parameters (Y) with stored data elements (V).
    """

    def __init__(
        self,
        data: Optional[S] = None,
        annotations: Optional[List[Y]] = None,
    ):
        """
        Initializes a AnnotatedDataCollection instance.

        Args:
            data (Optional[S]): The collection data.
            annotations (Optional[List[X]]): List of annotation parameters corresponding to each data element.
                Defaults to an empty list if not provided.
        """
        super().__init__(data)
        if annotations is None:
            annotations = []
        self.annotations: List[Y] = annotations

    def iter_with_annotations(self) -> Iterator[Tuple[V, Y]]:
        """
        Iterates over the elements of the AnnotatedDataCollection,
        returning tuples of (data element, corresponding annotation).

        Returns:
            Iterator[Tuple[V, Y]]: An iterator over pairs of data and annotation.
        """
        for item, param in zip(self, self.annotations):
            yield item, param

    def append(self, item: V) -> None:
        """
        Appends an element of type V to the collection, maintaining consistency.

        Args:
            item (V): The data element to append.

        Raises:
            ValueError: Appending without a annotation parameter is not allowed.
        """
        raise ValueError("Use append_with_annotation() to add data with annotation.")

    def append_with_annotation(self, item: V, annotation: Y) -> None:
        """
        Appends an element and its corresponding annotation parameter to the collection.

        Args:
            item (V): The data element to append.
            annotation (Y): The associated annotation parameter.
        """
        self.append(item)
        self.annotations.append(annotation)
