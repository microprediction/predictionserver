from predictionserver.futureconventions.sepconventions import SepConventions


class ObscurityHabits:

    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        self._OBSCURITY = 'ERIC_OBSCURITY_TEST'

    def obscurity(self):
        if self._OBSCURITY == "not obscure yet":
            raise RuntimeError('___obscurity property must be set before use')
        else:
            return self._OBSCURITY + SepConventions.sep()

    def set_obscurity(self, secret:str):
        self._OBSCURITY = secret
