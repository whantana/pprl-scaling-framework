plot_benchmarks <- function() {
    sizes <- c("10 KBytes","20 KBytes","50 KBytes")
    for (i in 1:3){
        scale <- read.csv(file=sprintf("benchmark_scale_%d.csv", i),sep=",",head=TRUE)
        max_y <- max(scale)
        k <- scale$k
        v1_times <- scale$createHashesV1
        v2_times <- scale$createHashesV2
        v3_times <- scale$createHashesV3
        mbV1_times <- scale$MBcreateHashesV1
        mbV2_times <- scale$MBcreateHashesV2
        mbV3_times <- scale$MBcreateHashesV3
        plot_colors <- c("blue","red","green","darkblue","darkred","darkgreen")
        png(filename=sprintf("benchmark_scale_%d.png", i), 
            height=1024, width=1024, bg="white")
        plot(v1_times, type="o", col=plot_colors[1], 
             ylim=c(0,max_y), axes=FALSE, ann=FALSE)
        axis(1, at=k)
        axis(2, las=1, at=4*i*0:max_y)
        box()
        lines(v2_times, type="o", col=plot_colors[2])
        lines(v3_times, type="o", col=plot_colors[3])
        lines(mbV1_times, type="o",col=plot_colors[4])
        lines(mbV2_times, type="o",col=plot_colors[5])
        lines(mbV3_times, type="o",col=plot_colors[6])
        title(xlab= "K")
        title(ylab= "Milliseconds")
        title(main=sprintf("Create Hashes Benchmark \n Iteration input : %s of bigrams",sizes[i]), col.main="black", font.main=4)
        
        legend(1, max_y, 
               c("createHashesV1",
                 "createHashesV2"
                 "createHashesV3",
                 "Dictionary Backed createhashesV1",
                 "Dictionary Backed createhashesV2",
                 "Dictionary Backed createhashesV3"),
               cex=1.1, col=plot_colors, pch=21:23, lty=1:3);
        dev.off()
    }
}