import pymorton
import math
from typing import List
from predictionserver.futureconventions.statsconventions import StatsConventions


class ZCurveConventions():
    """ Conventions for projections R^2->R and R^3->R """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    # Z-curve calculations

    @staticmethod
    def to_zscores(prctls):
        norminv = StatsConventions._norminv_function()
        return [norminv(p) for p in prctls]

    @staticmethod
    def morton_scale(dim):
        return 2 ** 10

    @staticmethod
    def morton_large(dim):
        SCALE = ZCurveConventions.morton_scale(dim=dim)
        return pymorton.interleave(*[SCALE - 1 for _ in range(dim)])

    def to_zcurve(self, prctls: List[float]):
        """ A mapping from R^n -> R based on the Morton z-curve """
        SAFE = False
        dim = len(prctls)
        if dim == 1:
            return self.to_zscores(prctls)[0]
        else:
            SCALE = self.morton_scale(dim)
            int_prctls = [int(math.floor(p * SCALE)) for p in prctls]
            m1 = pymorton.interleave(*int_prctls)
            if SAFE:
                int_prctls_back = pymorton.deinterleave2(
                    m1) if dim == 2 else pymorton.deinterleave3(m1)
                assert all(i1 == i2 for i1, i2 in zip(int_prctls, int_prctls_back))
            m2 = pymorton.interleave(*[SCALE - 1 for _ in range(dim)])
            zpercentile = m1 / m2
            return StatsConventions.norminv(zpercentile)

    def from_zcurve(self, zvalue, dim):
        zpercentile = StatsConventions.normcdf(zvalue)
        SCALE = self.morton_scale(dim)
        zmorton = int(self.morton_large(dim) * zpercentile + 0.5)
        if dim == 2:
            values = pymorton.deinterleave2(zmorton)
        elif dim == 3:
            values = pymorton.deinterleave3(zmorton)
        else:
            raise NotImplementedError('Only 2d or 3d')
        prtcls = [v / SCALE for v in values]
        return prtcls
