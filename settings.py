from decouple import config

DEBUG = config('DEBUG', cast=bool)
DATABASES = {
    'default': {
        'NAME': config('DB_NAME'),
        'USER': config('DB_USER'),
        'PASSWORD': config('DB_PASSWORD'),
        'HOST': config('DB_HOST'),
        'PORT': '',
    }
}