
library(Fluid)

Fluid::connect("127.0.0.1",51515)
t <- function() { print("Hello") }
d <- data.frame(x=1, y=1:10)
Fluid::submitJob(t,d)
