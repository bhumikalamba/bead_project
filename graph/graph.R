rm(list =ls())

library(igraph)
library(tidyverse)


setwd('/Users/rais/Desktop/post grad/EBAC/2020/CA/canary_v1/bead_project/graph')

df <- read.csv("User-graph.csv")


df$From <- as.character(df$From)
df$To <- as.character(df$To)

df <- df %>% select(From,To,Weight)
str(df)

g <- graph.data.frame(df, directed=TRUE)


#::::::::::::::::::::::::::::::::
# define function to plot a graph
#::::::::::::::::::::::::::::::::
plotG <- function(g) {
  plot(g, 
       # force-directed layout
       #layout=layout.fruchterman.reingold,
       #layout=layout_with_fr,
       vertex.size=4, 
       vertex.label=NA,
       #vertex.label.font=2, 
       #vertex.label.dist=0.5,
       vertex.color="blue",
       vertex.frame.color=FALSE,
       edge.arrow.size=0.1,
       arrow.width=0.2,
       edge.color="red")
}

plotG(g)


measures <- data.frame(twitter_handle_id = unique(c(df$From, df$To)))

#::::::::::::::::::::::::::::::::
# centrality measures
#::::::::::::::::::::::::::::::::

# degree centrality
degree_total <- degree(g,mode="total")
degree_total <- as.data.frame(degree_total)
degree_total <- cbind(twitter_handle_id = rownames(degree_total), degree_total)

degree_in <- degree(g,mode ="in")
degree_in <- as.data.frame(degree_in)
degree_in <- cbind(twitter_handle_id = rownames(degree_in), degree_in)

degree_out <- degree(g,mode="out")
degree_out <- as.data.frame(degree_out)
degree_out <- cbind(twitter_handle_id = rownames(degree_out), degree_out)

measures <- left_join(measures, degree_total, by = c('twitter_handle_id'))
measures <- left_join(measures, degree_in, by = c('twitter_handle_id'))
measures <- left_join(measures, degree_out, by = c('twitter_handle_id'))


#deg.dist <- degree_distribution(g,mode="total",cumulative=T)
#plot(x=0:length(deg.dist), y=1-deg.dist, pch=19, cex=1.2, col="orange", 
#      xlab="Degree", ylab="Cumulative Frequency")

# PageRank
pr <- page_rank(g,damping = 0.85)$vector
pr <- as.data.frame(pr)
pr <- cbind(twitter_handle_id = rownames(pr), pr)

measures <- left_join(measures, pr, by = c('twitter_handle_id'))


View(measures)


#::::::::::::::::::::::::::::::::
# community detection measures
#::::::::::::::::::::::::::::::::

# maximum cliques
largest.cliques(g)

# Count adjacent triangles
adj_tri <- transitivity(g,type ="localundirected",isolates="zero")
adj_tri <- as.data.frame(adj_tri,as_ids(V(g)))
adj_tri <- cbind(twitter_handle_id = rownames(adj_tri), adj_tri)

measures <- left_join(measures, adj_tri, by = c('twitter_handle_id'))



write_csv(measures, 'graph_algo_results_r.csv')

