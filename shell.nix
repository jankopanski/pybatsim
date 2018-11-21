let
  kapack = import
    #~/Projects/kapack
    ( fetchTarball "https://github.com/oar-team/kapack/archive/master.tar.gz")
  { };
in
  kapack.pybatsim_dev
