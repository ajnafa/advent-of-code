from polars import from_records, Int64, col

# Read in the puzzle input
with open("2024/12-01/input.txt") as f:
    input = f.read().splitlines()

## Day 1: Historian Hysteria - Part 1

# Get the input lists into a data frame
input = [line.split("   ") for line in input]
input = from_records(input, columns=['left', 'right'], orient="row")

# Cast the columns to integers
left = input['left'].cast(Int64).sort(reverse=False)
right = input['right'].cast(Int64).sort(reverse=False)

# Calculate the sum of the absolute differences to get the total distance
total_dist = (right - left).abs().sum()
print(f"The total distance is {total_dist}")

## Day 1: Historian Hysteria - Part 2

# Get unique values from the left list
left = left.unique().to_frame()
right_counts = right.value_counts().rename({"right": "left"})

# Merge the left and right counts
similarity = (
    left
    .join(right_counts, on='left', how='left')
    .fill_null(0)
    .with_columns(
        (col("left") * col("counts")).alias("similarity")
    )
)

# Sum the similarity to get the answer
similarity_score = similarity['similarity'].sum()
print(f"The similarity score is {similarity_score}")
