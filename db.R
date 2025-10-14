### INSTALLATION ###

# referenced during namespace check
required_packages <- c("RSQLite", "dplyr", "ggplot2", "sf","jsonlite","tidyr","purrr")

# check namespace for package
for (pkg in required_packages) {
  # install if not found
  if (!requireNamespace(pkg, quietly = TRUE)) {
    install.packages(pkg)
  }
}

### SOURCING ###

# load each required package
for (pkg in required_packages) {
  # Load the package using library()
  library(pkg, character.only = TRUE)
}


### CONNECTING ###

# connect to db
conn <- dbConnect(SQLite(), "~/Documents/GitHub/overdoses/od.db")


### LOADING ###

# database 👉 dataframes
counties <- dbReadTable(conn, "county")
countymonths <- dbReadTable(conn, "countymonth")
countyyears <- dbReadTable(conn, "countyyear")
states <- dbReadTable(conn, "state")
statemonths <- dbReadTable(conn, "statemonth")
stateyears <- dbReadTable(conn, "stateyear")


### PROCESSING ###
# 
aca <- map(states$kff_aca_exp, fromJSON, simplifyDataFrame=TRUE)
aca_named <- set_names(aca, states$name)

# nflis
# Parse the JSON string in each state row and combine with state name
nflis_flat_df <- map2_dfr(states$nflis_drug_reports, states$name, ~ {
# Parse JSON array string
df <- fromJSON(.x, simplifyDataFrame = TRUE)
# Add state name column
df$state <- .y
df
})

# unnest millenium json from stateyears
stateyears <- stateyears %>%
  # load millenium json into millj
  mutate(millj = lapply(millenium, fromJSON)) %>%
  # Make each analyte a row
  unnest_longer(millj) %>%
  # Flatten analyte fields into columns
  unnest_wider(millj) %>%
  pivot_wider(
    names_from = analyte,
    # all millj columns except these key columns
    values_from = -c(Year, State, analyte),
    # put column names together based on pivots
    names_glue = "millenium_{tolower(analyte)}_{.value}"
  )


### DOCUMENTING ###

cat("

#######################################################

📀 connected to od.db


💾 this session has preloaded the following dataframes:
    ~ counties
    ~ states
    ~ countymonths
    ~ statemonths
    ~ stateyears
    ~ aca_named


🌎 with $geojson fields on:
    states
    counties


e.g.:
    ```
    # Convert GeoJSON strings to sf objects
    states_sf <- do.call(rbind, lapply(states$geojson, function(gj) {
      st_read(gj, quiet = TRUE)
    }))

    # Plot the map
    ggplot(states_sf) +
      geom_sf(fill = 'white', color = 'black') +  # basic styling
      theme_void()  # remove axes and background
    ```

💼 JSON fields include states$nflis_drug_reports and others. Unpack them e.g.: 

    ```
    states$parsed_nflis <- lapply(states$nflis_drug_reports, function(x) fromJSON(x, flatten = TRUE))
    ```
#######################################################



    ")
