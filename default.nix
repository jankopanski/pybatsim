with import <nixpkgs> {};
with pkgs.python36Packages;

buildPythonPackage rec {
  name = "pybatsim";
  src = ./batsim;
  propagatedBuildInputs = with python36Packages; [
      sortedcontainers
      pyzmq
      redis
      click
      pandas
      docopt
      # for testing
      coverage
      pytest
    ];

}
