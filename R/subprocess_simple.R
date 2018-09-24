#' Run R-Scripts In Parallell
#'
#' `subprocess_simple` runs scripts in multiple processes but without duplicates.
#'
#' R scripts are run using Rscript.exe. Only one script with the same name, can run at a time.
#' All scripts that have different names will run in parallell, with a maximum of \code{process_limit} scripts.
#' Print-out of stdin and stderr from each process carries a small time penalty.
#' NB: Always end the scripts with "quit(save = 'no')" !
#'
#' @param script_paths (character) Paths to R-scripts.
#' @param working_directory (character) Working directory for scripts. Recycled if neccessary.
#' @param process_limit (integer) Number of processes to run in parallell (i.e. Rscript.exe instances).
#' @param overall_timeout (integer) Maximum run time minutes for all scripts, after that it kills all processes.
#' @param print_stdout (logical) Print standard out from process?
#' @param print_stderr (logical) Print standard error from process?
#' @param debuglevel (integer) Print debug messages, 0 (nothing) to 3 (maximum).
#' @return Value returned is vector of \code{exit_codes} for all
#' (so far completed) scripts.
#' @keywords subprocess_simple
#' @export
#' @examples
#' \dontrun{
#' tmp1 <- c(
#' "print('Hello World! 1'); print(Sys.time()) Sys.sleep(sample(x = 3:10, size = 1)); quit(save = 'no')",
#' "print('Hello World! 2'); Sys.sleep(sample(x = 3:10, size = 1)); quit(save = 'no')",
#' "print('Hello World! 3'); Sys.sleep(sample(x = 3:10, size = 1)); quit(save = 'no')",
#' "print('Hello World! err'); Sys.sleep(sample(x = 3:10, size = 1)); stop('error here.'); quit(save = 'no')"
#' )
#' tmpf1 <- c(
#'   tempfile(fileext = ".R"),
#'   tempfile(fileext = ".R"),
#'   tempfile(fileext = ".R"),
#'   tempfile(fileext = ".R"))
#' for(i in 1:4) writeLines(text = tmp1[i], con = tmpf1[i])
#' subprocess_simple(script_paths = c(tmpf1, tmpf1),
#'                  working_directory = c("."),
#'                  process_limit = 5,
#'                  overall_timeout = 15,
#'                  print_stdout = T,
#'                  print_stderr = T,
#'                  debuglevel = 0)
#' }
#'

subprocess_simple <- function(script_paths, working_directory, process_limit = 3,
                              overall_timeout = 60, print_stdout = F, print_stderr = F, debuglevel = 0){

  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Libs
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  require(subprocess)

  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Functions
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Function: Debug print function
  dp <- function(debuglevel_limit, message){if(debuglevel >= debuglevel_limit) print(message)}
  # Function: to get all data from slots
  slots_state <- function() {
    return(data.frame(slot = 1:process_limit,
                      process_state =   sapply(slots, function(x){ifelse(subprocess::is_process_handle(x), subprocess::process_state(x), "")}),
                      script = sapply(slots, function(x){ifelse(subprocess::is_process_handle(x), x$argument[2], "")}),
                      return_code = sapply(slots, function(x){ifelse(subprocess::is_process_handle(x),
                                                                     ifelse(subprocess::process_state(x) %in% c("exited", "terminated"),
                                                                            subprocess::process_return_code(x), as.integer(NA)), as.integer(NA))}),
                      c_handle = sapply(slots, function(x){ifelse(subprocess::is_process_handle(x),
                                                                  x$c_handle, as.integer(NA))}),
                      free = sapply(slots, function(x){ifelse(subprocess::is_process_handle(x),
                                                              F, T)})
    , stringsAsFactors = F)
    )}
  # Function: Print std out
  slots_read <- function(){
    lapply(slots, function(x){if(subprocess::is_process_handle(x)){
      return(subprocess::process_read(x))} else return(NULL) })}


  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Data sets used during loop
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  results <- data.frame() # Save results.
  scripts <- data.frame(script_paths = script_paths, working_directory = working_directory, stringsAsFactors = F) # Scripts to loop through.
  slots <- vector("list", length = process_limit) # The vector of process slots.
  starttime <- Sys.time() # Timeout counter started here!

  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Loop while there are jobs left or running
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  while(nrow(scripts) > 0 | any(!slots_state()$free)){


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Run script on empty slot
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    if(any(sapply(slots, is.null))){
      freeslot <- which(slots_state()$free)[1] # List available slots.
      is_ok_to_run <- !(scripts$script_paths %in% slots_state()$script) # List available scripts.
      if((length(freeslot) == 1) & any(is_ok_to_run) ){ # Check Slots available and scripts available.
        to_run <- scripts[min(which(is_ok_to_run)),] # Take the first available scripts.
        dp(1, "Slot started.")
        slots[[freeslot]] <- subprocess::spawn_process(command = "C:\\R-3.4.2\\bin\\Rscript.exe",
                                                       arguments = c("--max-mem-size=2000M", to_run$script_paths  ),
                                                       workdir = to_run$working_directory )
        dp(2, "Script started.")
        dp(2, paste(to_run$script_paths))
      } # if
    } # if


    #Debug print.
    dp(3, "~~~~~~~~~~~~~~~~~~~~~~~~")
    dp(3, slots_state() )
    dp(3, "~~~~~~~~~~~~~~~~~~~~~~~~")


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # PRint stdout or stderr
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    if(print_stdout | print_stderr){
      pri <- slots_read()
      for(i in 1:length(pri)){
        if(!is.null(pri[[i]])){

          if(print_stdout & length(pri[[i]]$stdout > 0) ){
            cat(paste("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n"))
            cat(paste("Slot ", i, "STDOUT : ", basename(slots_state()$script[i]), "\n"))
            print(pri[[i]]$stdout)
          }
          if(print_stderr & length(pri[[i]]$stderr > 0) ){
            cat(paste("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n"))
            cat(paste("Slot ", i, "STDERR : ", basename(slots_state()$script[i]), "\n"))
            print(pri[[i]]$stderr)
          }
        }
      }
    }


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Clear slots and script rows
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    is_finished = slots_state()$process_state %in% c("exited", "terminated") # Check which are finished.
    if(any(is_finished)){
      dp(1, "Slot finished.")
      # Get slot content.
      dp(2, paste("Exit code:", slots_state()$return_code))
      dp(3, paste("Script:", basename(slots_state()$script)))

      # Remove finished script from the data frame.
      finished <- which(is_finished)
      for(i in 1:length(finished)){
        rem <- min(which(scripts$script_paths %in% slots_state()$script[finished]))
        if(!is.na(rem)) {
          dp(2, paste("Removed script."))
          dp(3, paste("Script:", scripts[rem, "script_paths"]))
          scripts <- scripts[-rem, ]    # Remove 1 instance of the script list.
        }
      }
      dp(3, paste("Scripts remaining:", nrow(scripts)))

      # Add exitcode to results. Clear finished slots.
      results <- unlist(c(results, unlist(slots_state()$return_code[is_finished] )))
      slots[is_finished] <- list(NULL)
    }#if


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Sleep for 1 sec.
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #cat("\n\n\n\n\n")
    Sys.sleep(1)
    if(overall_timeout < as.integer(difftime(time2 = starttime , time1 = Sys.time(), units = "sec"))){
      print(paste("TIMEOUT REACHED!"))
      print("Kill all slots...")
      tmp <- lapply(slots, function(x){if(subprocess::is_process_handle(x)){
        subprocess::process_kill(x)}  })
      break;
    }
  } # while
  return(results)
} #function
