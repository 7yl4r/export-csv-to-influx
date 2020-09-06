

time_column = "time"
row = {time_column: 99}


timestamp_str = str(float(row[time_column]))  # raises if not posix-time-like
timestamp_magnitude = len(timestamp_str.split('.')[0])
timestamp_remove_decimal = int(str(timestamp_str).replace('.', ''))
# add zeros to convert to nanoseconds
timestamp_influx = ('{:<0'+str(9+timestamp_magnitude)+'d}').format(
    timestamp_remove_decimal
)
timestamp = int(timestamp_influx)


print(timestamp)
print(len(str(timestamp)))
