from io import TextIOBase, TextIOWrapper
from typing import Optional


class StrIOGenerator(TextIOBase):
    def __init__(self, binary_chunk, text_enc):
        self._iter = TextIOWrapper(binary_chunk, encoding=text_enc)
        self._buff = ''

    def readable(self) -> bool:
        return True

    def _wrapper(self, n: Optional[int] = None) -> str:
        while not self._buff:
            try:
                self._buff = next(self._iter)
            except StopIteration:
                break
        ret = self._buff[:n]
        self._buff = self._buff[len(ret):]
        return ret

    def read(self, n: Optional[int] = None) -> str:
        line = []
        if n is None or n < 0:
            while True:
                m = self._wrapper()
                if not m:
                    break
                line.append(m)
        else:
            while n > 0:
                m = self._wrapper(n)
                if not m:
                    break
                n -= len(m)
                line.append(m)
        return ''.join(line)
