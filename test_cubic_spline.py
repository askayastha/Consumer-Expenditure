from scipy.interpolate import CubicSpline
from scipy.interpolate import UnivariateSpline
from scipy.interpolate import LSQUnivariateSpline
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import config
import os

export_file = os.path.join(config.EXPORT_FILES_PATH, "mtbi_avg_spend_intrvw_{}_to_{}.csv".format(2014, 2016))
ucc = 360320

# x = np.arange(10)
# y = np.sin(x)
# cs = CubicSpline(x, y)
# xs = np.arange(-0.5, 9.6, 0.1)
# plt.figure(figsize=(6.5, 4))
# plt.plot(x, y, 'o', label='data')
# plt.plot(xs, cs(xs), label="S")
# plt.xlim(-0.5, 9.5)
# plt.legend(loc='lower left', ncol=2)
# plt.show()

test_pipe = pd.read_csv('/Volumes/Transcend/pumd_data_files/processed_data_5yrs_bucket_jun20/mtbi_avg_spend_intrvw_2011_to_2015.csv')
test_pipe = test_pipe[test_pipe['UCC'] == ucc]

x = test_pipe['AGE_REF']
y = test_pipe['AVG_SPEND']
plt.plot(x, y, 'ro', ms=5)

# c_spline = CubicSpline(x, y)
# xs = np.linspace(20, 80, 1000)
# plt.plot(xs, c_spline(xs), 'b')

# s=33500000
for i in range(0, 99999999, 1000):
    u_spline = UnivariateSpline(x, y, s=i)
    knot_count = len(u_spline.get_knots())
    print(knot_count)
    if knot_count <= 8:
        break
xs = np.linspace(20, 80, 1000)
plt.plot(xs, u_spline(xs), 'b', lw=3)

# u_spline.set_smoothing_factor(0.5)
# plt.plot(xs, u_spline(xs), 'g')
plt.title("Special lump sum mortgage payment (owned home) [2011-2015]")
plt.xlabel('Age')
plt.ylabel('Average Spend in $')
plt.show()

# x = np.linspace(30, 80, 50)
# y = np.exp(-x**3) + 0.1 * np.random.randn(50)
# plt.plot(x, y, 'ro', ms=5)
#
# spl = UnivariateSpline(x, y)
# xs = np.linspace(30, 80, 1000)
# plt.plot(xs, spl(xs), 'g')
#
# spl.set_smoothing_factor(1)
# plt.plot(xs, spl(xs), 'b')
# plt.show()
