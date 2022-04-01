class Coordinates:
    """Coordinates of longitude and latitude

    Different APIs accept and receive longitude and latitude as ordered pairs, but with different orders; this class
    removes ordering to make the meaning of each value explicit.
    """
    __slots__ = ('_longitude', '_latitude')

    def __init__(self, *, longitude: float, latitude: float):
        self._longitude = longitude
        self._latitude = latitude

    def repr(self):
        return f'Coordinates(longitude={self.longitude!r}, latitude={self.latitude!r})'

    @property
    def longitude(self) -> float:
        return self._longitude

    @property
    def latitude(self) -> float:
        return self._latitude
