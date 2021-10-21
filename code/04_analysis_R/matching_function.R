# Matching function borrowed from: https://nhsrcommunity.com/blog/exact-matching-in-r/ 

# data = dataset containing:
# - treatment/exposure variable 'mvar' (a string specifying variable name).
# - matching variable 'mvar' (a string specifying variable name). If you want to match on multiple variables, concatenate them first.
# other inputs are:
# - ratio of cases:controls (an integer > 0)
# - seed for fixing random selection of cases/controls (an integer; default NULL means no seed). Choice of seed is arbitrary.
# returns data.table of matched observations, with additional variable 'id' for use in paired/grouped analyses

smatch <- function (data, treat, mvar, ratio = 1, seed = NULL) {
  setnames(data, mvar, '.mvar')
  targ <- data[, .(case = sum(get(treat)), control = sum(!get(treat))), .mvar]
  targ[, cst := floor(pmin(control / ratio, case))]
  targ[, cnt := cst * ratio]
  targ <- targ[cst > 0]
  l2 <- cumsum(targ$cst)
  ids <- mapply(':', c(0, l2[-nrow(targ)]), l2-1)
  names(ids) <- targ$.mvar
  case <- NULL
  control <- NULL
  x <- .Random.seed
  set.seed(seed)
  on.exit({.Random.seed <- x})
  for(i in targ$.mvar) {
    case[[i]] <- data[get(treat) == T & .mvar == i][sample(.N, targ$cst[targ$.mvar == i])]
    case[[i]][, id := ids[[i]]]
    control[[i]] <- data[get(treat) == F & .mvar == i][sample(.N, targ$cnt[targ$.mvar == i])]
    control[[i]][, id := rep(ids[[i]], each = ratio)]
  }
  rbindlist(c(case, control))
}

