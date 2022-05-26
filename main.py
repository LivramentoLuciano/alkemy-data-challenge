from utils import *
import pandas as pd

# Cargo los datos al dia de la fecha
museums = load_data_museums()
# cinemas = load_data_cinemas()

# visualizo datos
if not museums.empty:
    print(museums)

# if not cinemas.empty:
#     print(cinemas)

