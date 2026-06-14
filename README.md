# ParallelProgramming
A parallel file search tool that uses MPI and OpenMP to search for files across the entire filesystem from root (/) directory.
<br/><br/>
<img width="1282" height="253" alt="filename" src="https://github.com/user-attachments/assets/7f5aac61-e09d-4274-a033-7af0a667832e" />
<br/>
<img width="947" height="412" alt="extension" src="https://github.com/user-attachments/assets/1fd8af0b-63e6-4576-a5bd-f48a0c3154fd" />
<br/><br/>

The parallelism is at two levels, MPI processes and OpenMP threads within each process. <br/>
The program runs with 8 MPI processes by default, you can change this with NP="x" etc. <br/>
To change threads num: 
<code> export OMP_THREADS_NUM="x" </code>
<br/><br/>
<img width="610" height="380" alt="np" src="https://github.com/user-attachments/assets/47ec8cc3-25c1-494a-8aef-2c969a427579" />
