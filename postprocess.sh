find . -name "part-r-*" | xargs cat > merged
sort -t" " -k2nr,2 merged >> result.txt