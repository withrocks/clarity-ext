context.outfile.name = "filter_row_column.csv"
context.outfile.write_line("key,sample")

# Show all wells in column 1 and 2, but skip row B
for well in context.plate.list_wells():
    # NOTE: One would perhaps rather use list comprehensions, but this is supposed to be very readable:
    if (well.col == 1 or well.col == 2) and (well.row != "B"):
        line = "{}:{},{}".format(well.row, well.col, well.content or "0")
        context.outfile.write_line("{}".format(line))
