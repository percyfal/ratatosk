
%.pdf: src/%.tex
	pdflatex $<

%.jpg: %.pdf
	convert -verbose -density 150 -trim $< -quality 100 -sharpen 0x1.0 $@

%.png: %.pdf
	convert -verbose -density 600 -trim $< -quality 100 -sharpen 0x1.0 $@

%.txt.gz: %.txt
	gzip -v $<
