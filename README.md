# Histograph Data - GeoNames data module

Downloads and converts data from GeoNames, see https://github.com/histograph/data for details.

## Configuration

Downloads complete GeoNames dump (`allCountries.zip`), and creates NDJSON files containing PITs and relations. In `data` section of [Histograph configuration file](https://github.com/histograph/config), you can specify which countries are processed.

A selection of GeoNames URIs (e.g. places, countries) that lie outside of this country can be imported as well by listing them in `extra-uris.json`. By default, `extra-uris.json` contains all the world's countries and 5000 biggest cities.

```yml
data:
  modules:
    geonames:
      countries:
        - NL

      extraUris: ./extra-uris.json
```

See https://github.com/histograph/config.

## License

Copyright (C) 2015 [Waag Society](http://waag.org).

MIT
